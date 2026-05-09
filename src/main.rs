use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use nnnoiseless::DenoiseState;
use serde::Deserialize;
use tokio::sync::{mpsc, RwLock};
use tower_http::{services::ServeDir, trace::TraceLayer};
use tracing::{error, info};
use uuid::Uuid;

const SAMPLE_RATE: usize = 48_000;
const FRAME_SIZE: usize = 480;
const FRAME_BYTES: usize = FRAME_SIZE * 2;

#[derive(Clone)]
struct AppState {
    rooms: Arc<RwLock<HashMap<String, Room>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            rooms: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

struct Room {
    clients: HashMap<Uuid, ClientConnection>,
}

#[derive(Clone)]
struct ClientConnection {
    display_name: String,
    tx: mpsc::UnboundedSender<Message>,
}

#[derive(Debug, Deserialize)]
struct WsParams {
    room: Option<String>,
    name: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "denoise_im_server=info,tower_http=info".to_string()),
        )
        .init();

    let state = AppState::default();

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .nest_service("/", ServeDir::new("static").append_index_html_on_directories(true))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind listener");

    info!(
        "server started at http://127.0.0.1:3000, expected audio format: {} Hz mono, {} samples/frame",
        SAMPLE_RATE, FRAME_SIZE
    );

    axum::serve(listener, app).await.expect("server crashed");
}

async fn ws_handler(
    State(state): State<AppState>,
    Query(params): Query<WsParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let room = sanitize_room(params.room.as_deref().unwrap_or("main"));
    let name = sanitize_name(params.name.as_deref().unwrap_or("anonymous"));
    ws.on_upgrade(move |socket| handle_socket(state, socket, room, name))
}

async fn handle_socket(state: AppState, socket: WebSocket, room: String, name: String) {
    let client_id = Uuid::new_v4();
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();

    let write_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if ws_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    state
        .add_client(&room, client_id, ClientConnection {
            display_name: name.clone(),
            tx: out_tx.clone(),
        })
        .await;

    info!("client connected: room={room}, name={name}, id={client_id}");
    let mut denoise_state = DenoiseState::new();
    let mut drop_first_output = true;

    while let Some(msg_result) = ws_rx.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(err) => {
                error!("websocket receive error ({client_id}): {err}");
                break;
            }
        };

        match msg {
            Message::Binary(bin) => {
                if bin.len() != FRAME_BYTES {
                    continue;
                }
                let cleaned = denoise_bytes(&mut denoise_state, &bin);
                if drop_first_output {
                    drop_first_output = false;
                    continue;
                }
                state.broadcast_audio(&room, client_id, cleaned).await;
            }
            Message::Ping(payload) => {
                let _ = out_tx.send(Message::Pong(payload));
            }
            Message::Close(_) => break,
            Message::Text(_) | Message::Pong(_) => {}
        }
    }

    state.remove_client(&room, client_id).await;
    write_task.abort();
    info!("client disconnected: room={room}, id={client_id}");
}

impl AppState {
    async fn add_client(&self, room: &str, client_id: Uuid, client: ClientConnection) {
        let mut rooms = self.rooms.write().await;
        let room_state = rooms.entry(room.to_string()).or_insert_with(|| Room {
            clients: HashMap::new(),
        });
        room_state.clients.insert(client_id, client);
    }

    async fn remove_client(&self, room: &str, client_id: Uuid) {
        let mut rooms = self.rooms.write().await;
        if let Some(room_state) = rooms.get_mut(room) {
            room_state.clients.remove(&client_id);
            if room_state.clients.is_empty() {
                rooms.remove(room);
            }
        }
    }

    async fn broadcast_audio(&self, room: &str, sender: Uuid, payload: Vec<u8>) {
        let recipients = {
            let rooms = self.rooms.read().await;
            rooms
                .get(room)
                .map(|room_state| {
                    room_state
                        .clients
                        .iter()
                        .filter(|(client_id, _)| **client_id != sender)
                        .map(|(_, client)| (client.display_name.clone(), client.tx.clone()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        for (name, tx) in recipients {
            if tx.send(Message::Binary(payload.clone().into())).is_err() {
                error!("failed to push audio frame to client {name} in room {room}");
            }
        }
    }
}

fn denoise_bytes(state: &mut DenoiseState, input_bytes: &[u8]) -> Vec<u8> {
    let mut input = [0.0f32; FRAME_SIZE];
    for (i, chunk) in input_bytes.chunks_exact(2).enumerate() {
        let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
        input[i] = sample as f32;
    }

    let mut output = [0.0f32; FRAME_SIZE];
    let _vad_prob = state.process_frame(&mut output, &input);
    pcm_to_bytes(&output)
}

fn pcm_to_bytes(frame: &[f32; FRAME_SIZE]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_BYTES);
    for &sample in frame {
        let int_sample = sample.clamp(i16::MIN as f32, i16::MAX as f32).round() as i16;
        out.extend_from_slice(&int_sample.to_le_bytes());
    }
    out
}

fn sanitize_room(raw: &str) -> String {
    sanitize_token(raw, "main")
}

fn sanitize_name(raw: &str) -> String {
    sanitize_token(raw, "anonymous")
}

fn sanitize_token(raw: &str, default: &str) -> String {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        .take(32)
        .collect();

    if cleaned.is_empty() {
        default.to_string()
    } else {
        cleaned
    }
}
