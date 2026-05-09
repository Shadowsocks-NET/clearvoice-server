use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use nnnoiseless::DenoiseState;
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Deserialize, Default)]
struct ServerConfig {
    #[serde(default, rename = "listenAddresses")]
    listen_addresses: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RoomsResponse {
    rooms: Vec<RoomSummary>,
}

#[derive(Debug, Serialize)]
struct RoomSummary {
    name: String,
    online: usize,
}

#[derive(Debug, Serialize)]
struct RoomUsersResponse {
    room: String,
    users: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientWsEvent {
    Ping { seq: u64, ts: u64 },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerWsEvent {
    Pong { seq: u64, ts: u64 },
    RoomUsers { room: String, users: Vec<String> },
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
        .route("/api/rooms", get(rooms_handler))
        .route("/api/rooms/{room}/users", get(room_users_handler))
        .nest_service("/", ServeDir::new("static").append_index_html_on_directories(true))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let listen_addrs = load_listen_addrs().unwrap_or_else(|message| {
        eprintln!("failed to load listen addresses: {message}");
        eprintln!("configure listen addresses in ./config.json with field listenAddresses");
        std::process::exit(2);
    });

    let mut listeners = Vec::with_capacity(listen_addrs.len());
    for addr in listen_addrs {
        let listener = bind_listener(addr).unwrap_or_else(|err| {
            eprintln!("failed to bind listener {addr}: {err}");
            std::process::exit(1);
        });
        info!(
            "server started at http://{}, expected audio format: {} Hz mono, {} samples/frame",
            addr, SAMPLE_RATE, FRAME_SIZE
        );
        listeners.push((addr, listener));
    }

    let mut servers = tokio::task::JoinSet::new();
    for (addr, listener) in listeners {
        let app = app.clone();
        servers.spawn(async move {
            axum::serve(listener, app)
                .await
                .map_err(|err| format!("server at http://{addr} crashed: {err}"))
        });
    }

    if let Some(result) = servers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                eprintln!("{message}");
                std::process::exit(1);
            }
            Err(err) => {
                eprintln!("server task join error: {err}");
                std::process::exit(1);
            }
        }
    }
}

fn bind_listener(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket =
        socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;

    if let SocketAddr::V6(v6_addr) = addr {
        if v6_addr.ip().is_unspecified() {
            socket.set_only_v6(false)?;
        }
    }

    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    tokio::net::TcpListener::from_std(socket.into())
}

fn load_listen_addrs() -> Result<Vec<SocketAddr>, String> {
    const CONFIG_PATH: &str = "config.json";
    const DEFAULT_LISTEN_ADDR: &str = "[::]:3000";

    let config = load_config(CONFIG_PATH)?;
    let mut addrs = Vec::new();

    if config.listen_addresses.is_empty() {
        push_listen_addr(&mut addrs, DEFAULT_LISTEN_ADDR)?;
        return Ok(addrs);
    }

    for value in config.listen_addresses {
        push_listen_addr(&mut addrs, &value)?;
    }

    Ok(addrs)
}

fn load_config(path: &str) -> Result<ServerConfig, String> {
    let raw = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ServerConfig::default());
        }
        Err(err) => return Err(format!("failed to read {path}: {err}")),
    };

    serde_json::from_str::<ServerConfig>(&raw)
        .map_err(|err| format!("failed to parse {path} as JSON: {err}"))
}

fn push_listen_addr(addrs: &mut Vec<SocketAddr>, value: &str) -> Result<(), String> {
    let addr = parse_socket_addr(value)?;
    if !addrs.contains(&addr) {
        addrs.push(addr);
    }
    Ok(())
}

fn parse_socket_addr(value: &str) -> Result<SocketAddr, String> {
    value.parse::<SocketAddr>().map_err(|_| {
        format!(
            "invalid listen address: {value} (expected ip:port, e.g. [::]:3000 or 0.0.0.0:3000)"
        )
    })
}

async fn rooms_handler(State(state): State<AppState>) -> Json<RoomsResponse> {
    Json(RoomsResponse {
        rooms: state.rooms_snapshot().await,
    })
}

async fn room_users_handler(
    State(state): State<AppState>,
    Path(room): Path<String>,
) -> Json<RoomUsersResponse> {
    let room = sanitize_room(&room);
    Json(RoomUsersResponse {
        users: state.room_users_snapshot(&room).await,
        room,
    })
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
        .add_client(
            &room,
            client_id,
            ClientConnection {
                display_name: name.clone(),
                tx: out_tx.clone(),
            },
        )
        .await;

    state.broadcast_room_users(&room).await;

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
            Message::Text(text) => {
                if let Ok(event) = serde_json::from_str::<ClientWsEvent>(text.as_ref()) {
                    match event {
                        ClientWsEvent::Ping { seq, ts } => {
                            send_ws_event(&out_tx, &ServerWsEvent::Pong { seq, ts });
                        }
                    }
                }
            }
            Message::Ping(payload) => {
                let _ = out_tx.send(Message::Pong(payload));
            }
            Message::Close(_) => break,
            Message::Pong(_) => {}
        }
    }

    state.remove_client(&room, client_id).await;
    state.broadcast_room_users(&room).await;
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

    async fn rooms_snapshot(&self) -> Vec<RoomSummary> {
        let rooms = self.rooms.read().await;
        let mut out = rooms
            .iter()
            .map(|(name, room)| RoomSummary {
                name: name.clone(),
                online: room.clients.len(),
            })
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    async fn room_users_snapshot(&self, room: &str) -> Vec<String> {
        let rooms = self.rooms.read().await;
        let mut users = rooms
            .get(room)
            .map(|room_state| {
                room_state
                    .clients
                    .values()
                    .map(|client| client.display_name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        users.sort();
        users
    }

    async fn broadcast_room_users(&self, room: &str) {
        let (users, recipients) = {
            let rooms = self.rooms.read().await;
            let Some(room_state) = rooms.get(room) else {
                return;
            };

            let mut users = room_state
                .clients
                .values()
                .map(|client| client.display_name.clone())
                .collect::<Vec<_>>();
            users.sort();

            let recipients = room_state
                .clients
                .values()
                .map(|client| client.tx.clone())
                .collect::<Vec<_>>();

            (users, recipients)
        };

        let event = ServerWsEvent::RoomUsers {
            room: room.to_string(),
            users,
        };

        for tx in recipients {
            send_ws_event(&tx, &event);
        }
    }
}

fn send_ws_event(tx: &mpsc::UnboundedSender<Message>, event: &ServerWsEvent) {
    match serde_json::to_string(event) {
        Ok(payload) => {
            let _ = tx.send(Message::Text(payload.into()));
        }
        Err(err) => {
            error!("failed to serialize ws event: {err}");
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
