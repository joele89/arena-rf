use axum::{
    Form, Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{Html, IntoResponse},
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use rosc::OscPacket;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::{RwLock, broadcast};
use url::Url;

// Embed the frontend interfaces straight into the compiled application binary
const DASHBOARD_HTML: &str = include_str!("dashboard.html");
const CONFIG_HTML: &str = include_str!("config.html");
const COMPY_BLUEPRINT_PATH: &str = "show_blueprint.json";

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Asset {
    id: String,
    name: String,
    performer: String,
    notes: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
struct SongState {
    title: String,
    incoming: Vec<Asset>,
    continuing: Vec<Asset>,
    outgoing: Vec<Asset>,
}

type ShowData = HashMap<i32, SongState>;

struct AppState {
    show_cache: RwLock<ShowData>,
    current_song_idx: RwLock<i32>,
    google_sheet_url: RwLock<String>,
    tx: broadcast::Sender<String>,
}

#[derive(Deserialize)]
struct SaveSheetRequest {
    sheet_url: String,
}

#[derive(Deserialize)]
struct TrackJumpRequest {
    song_index: i32,
}

#[tokio::main]
async fn main() {
    println!("🚀 Launching Monolithic Arena RF Server...");
    let (tx, _) = broadcast::channel(100);

    // POWER OUTAGE RECOVERY: Look for existing allocation data cache on disk
    let recovered_blueprint = match tokio::fs::read_to_string(COMPY_BLUEPRINT_PATH).await {
        Ok(data) => {
            println!("💾 Local Storage: Loaded backup allocation matrix from disk.");
            serde_json::from_str::<ShowData>(&data).unwrap_or_default()
        }
        Err(_) => {
            println!("ℹ️ No local spreadsheet backup found. Awaiting config linking.");
            HashMap::new()
        }
    };

    let shared_state = Arc::new(AppState {
        show_cache: RwLock::new(recovered_blueprint),
        current_song_idx: RwLock::new(1), // Boot up safely on Track 1
        google_sheet_url: RwLock::new("".to_string()),
        tx,
    });

    // ─── THREAD 1: OSC COMMAND ROUTER (QLAB / CUEPILOT INBOUND) ────────────
    let osc_state = Arc::clone(&shared_state);
    tokio::spawn(async move {
        let addr = "0.0.0.0:9001";
        let socket = std::net::UdpSocket::bind(addr).expect("Could not bind UDP OSC port");
        println!("📡 OSC Core listening on UDP://{}", addr);

        let mut buf = [0u8; 2048];
        loop {
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                if let Ok((_, OscPacket::Message(msg))) = rosc::decoder::decode_udp(&buf[..size]) {
                    if msg.addr == "/rf/song" {
                        if let Some(rosc::OscType::Int(new_idx)) = msg.args.first() {
                            update_and_broadcast_track(&osc_state, *new_idx).await;
                        }
                    }
                }
            }
        }
    });

    // ─── THREAD 2: EMBEDDED AXUM WEB INFRASTRUCTURE ────────────────────────
    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/ws", get(ws_handler))
        .route("/config", get(serve_config))
        .route("/config/sheet", post(handle_save_sheet))
        .route("/config/sync", post(handle_manual_sync))
        .route("/config/jump", post(handle_manual_jump))
        .with_state(shared_state);

    let web_addr = SocketAddr::from(([0, 0, 0, 0], 9000));
    println!("📺 Stage Displays: http://<server-ip>:9000");
    println!("⚙️ Admin Config Interface: http://<server-ip>:9000/config");

    let listener = tokio::net::TcpListener::bind(web_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ─── BROADCAST CORE ────────────────────────────────────────────────────────

async fn update_and_broadcast_track(state: &AppState, new_idx: i32) {
    let mut idx = state.current_song_idx.write().await;
    *idx = new_idx;
    println!(
        "🎵 Track Transition: Advancing venue screens to Index {}",
        new_idx
    );

    let payload = serde_json::json!({ "type": "TRACK_CHANGE", "song_index": new_idx }).to_string();
    let _ = state.tx.send(payload);
}

// ─── ADMIN HUB ROUTE HANDLERS ──────────────────────────────────────────────

async fn serve_dashboard() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}
async fn serve_config() -> impl IntoResponse {
    Html(CONFIG_HTML)
}

async fn handle_save_sheet(
    State(state): State<Arc<AppState>>,
    Form(payload): Form<SaveSheetRequest>,
) -> impl IntoResponse {
    if let Ok(parsed_url) = Url::parse(&payload.sheet_url) {
        let path_segments: Vec<&str> = parsed_url
            .path_segments()
            .map(|c| c.collect())
            .unwrap_or_default();
        if let Some(d_idx) = path_segments.iter().position(|&s| s == "d") {
            if let Some(sheet_id) = path_segments.get(d_idx + 1) {
                let mut url_guard = state.google_sheet_url.write().await;
                *url_guard = sheet_id.to_string();
                return format!("✅ Sheet ID Extracted & Saved: {}", sheet_id).into_response();
            }
        }
    }
    "❌ Invalid Link Structure. Please copy the complete browser URL directly.".into_response()
}

async fn handle_manual_jump(
    State(state): State<Arc<AppState>>,
    Form(payload): Form<TrackJumpRequest>,
) -> impl IntoResponse {
    update_and_broadcast_track(&state, payload.song_index).await;
    format!(
        "🚀 Global Jump Complete: Moved display grids to Track {}",
        payload.song_index
    )
}

// ─── 2D MATRIX SPREADSHEET PARSING LOGIC ───────────────────────────────────

async fn handle_manual_sync(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let active_id = state.google_sheet_url.read().await;
    if active_id.is_empty() {
        return "⚠️ Configuration Error: Please link a valid spreadsheet first.".into_response();
    }

    let client = reqwest::Client::new();
    let oauth_token = "YOUR_OAUTH_TOKEN_HERE"; // Populate with token or service account credentials

    // Request columns A through ZZ to pull the entire matrix configuration
    let url = format!(
        "https://googleapis.com{}/values/Matrix_Sheet!A:ZZ?valueRenderOption=UNFORMATTED_VALUE",
        *active_id
    );

    let response = match client.get(&url).bearer_auth(oauth_token).send().await {
        Ok(res) => match res.json::<serde_json::Value>().await {
            Ok(json) => json,
            Err(_) => {
                return "❌ Parsing Error: Spreadsheet tab formatting is invalid.".into_response();
            }
        },
        Err(_) => {
            return "❌ Network Error: Could not connect to Google API firewalls.".into_response();
        }
    };

    let rows = match response["values"].as_array() {
        Some(r) => r,
        None => {
            return "❌ API Error: 'Matrix_Sheet' tab not found or completely empty."
                .into_response();
        }
    };

    let headers = &rows[0];
    let total_cols = headers.as_array().unwrap().len();
    let mut updated_show_data: ShowData = HashMap::new();

    // Loop through columns starting at index 2 (the 3rd column = Song 1)
    for col_idx in 2..total_cols {
        let song_title = headers[col_idx]
            .as_str()
            .unwrap_or("Untitled Track")
            .to_string();
        let song_number = (col_idx - 1) as i32;

        let mut current_song_state = SongState::default();
        current_song_state.title = song_title;

        // Process asset allocation rows down the column matrix
        for row_idx in 1..rows.len() {
            let row = match rows[row_idx].as_array() {
                Some(r) => r,
                None => continue,
            };

            let gear_id = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let asset_name = row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if gear_id.is_empty() {
                continue;
            }

            // Evaluate current state against previous state row properties
            let current_user = row
                .get(col_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let previous_user = row
                .get(col_idx - 1)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let asset = Asset {
                id: gear_id,
                name: asset_name,
                performer: current_user.clone(),
                notes: "".to_string(),
            };

            // MATRIX STATE MAPPING LOGIC
            if !current_user.is_empty() && previous_user.is_empty() {
                // Was empty, now assigned = Incoming Handout
                current_song_state.incoming.push(asset);
            } else if !current_user.is_empty() && current_user == previous_user {
                // Performer hasn't changed = Continuing Stage State
                current_song_state.continuing.push(asset);
            } else if current_user.is_empty() && !previous_user.is_empty() {
                // Was assigned, now empty = Outgoing Handback
                let mut handback_asset = asset.clone();
                handback_asset.performer = previous_user;
                current_song_state.outgoing.push(handback_asset);
            } else if !current_user.is_empty()
                && !previous_user.is_empty()
                && current_user != previous_user
            {
                // Hot Swap changeover: Direct handover from artist A to B
                let mut swap_asset = asset.clone();
                swap_asset.notes = format!("HOT SWAP from {}", previous_user);
                current_song_state.incoming.push(swap_asset);
            }
        }

        updated_show_data.insert(song_number, current_song_state);
    }

    // Safely write the newly compiled matrix data back into thread-safe memory
    {
        let mut cache = state.show_cache.write().await;
        *cache = updated_show_data;
    }

    // Sync down to disk file storage for hard power outage protection
    if let Ok(json_data) = serde_json::to_string(&*state.show_cache.read().await) {
        let temp_path = format!("{}.tmp", COMPY_BLUEPRINT_PATH);
        if tokio::fs::write(&temp_path, json_data).await.is_ok() {
            let _ = tokio::fs::rename(temp_path, COMPY_BLUEPRINT_PATH).await;
        }
    }
    "⚡ Data-Sync Complete: Backstage displays updated across all local mirrors.".into_response()
}
// ─── LIVE WEBSOCKET COMMUNICATION STREAMING ────────────────────────────────
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

async fn handle_websocket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();
    // Fast-sync current frame settings upon canvas boot
    let init_payload = serde_json::json!({
        "type": "INITIAL_SYNC",
        "current_song_idx": *state.current_song_idx.read().await,
        "show_data": *state.show_cache.read().await
    })
    .to_string();
    if sender.send(Message::Text(init_payload)).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
        Ok(msg) = rx.recv() => {
            if sender.send(Message::Text(msg)).await.is_err() { break; }
        }
        Some(Ok(Message::Text(text))) = receiver.next() => {
            if let Ok(cmd) = serde_json::from_str::<serde_json::Value>(&text) {
                if cmd["type"] == "MANUAL_CHANGE" {
                    if let Some(target_idx) = cmd["song_index"].as_i64() {
                        let mut idx = state.current_song_idx.write().await;
                        *idx = target_idx as i32;
                        let broadcast_payload = serde_json::json!({
                            "type": "TRACK_CHANGE",
                            "song_index": *idx
                        }).to_string();
                        let _ = state.tx.send(broadcast_payload);
                    }
                }
            }
        }
        else => break,
        }
    }
}
