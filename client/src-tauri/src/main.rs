// Prevents additional console window on Windows in release
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tauri::{Emitter, Manager, State};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;

// --- Data Models ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ControlMessage {
    Register { subdomain: String },
    RegisterAck { subdomain: String, public_url: String },
    NewConnection { tunnel_id: String },
    ConnectionAck { tunnel_id: String },
    Ping,
    Pong,
}

#[derive(Debug, Clone, Serialize)]
struct LogEvent {
    level: String,
    message: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
struct StatusEvent {
    status: String,
    public_url: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ConnectionEvent {
    tunnel_id: String,
    timestamp: u64,
}

struct TunnelState {
    cancel_token: Option<CancellationToken>,
    public_url: Option<String>,
}

struct AppState {
    tunnel: Arc<Mutex<TunnelState>>,
}

// --- Tauri Commands ---

#[tauri::command]
async fn start_tunnel(
    local_port: u16,
    server_host: String,
    subdomain: String,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    let mut tunnel = state.tunnel.lock().await;
    
    if tunnel.cancel_token.is_some() {
        return Err("Tunnel is already running".to_string());
    }

    let cancel_token = CancellationToken::new();
    tunnel.cancel_token = Some(cancel_token.clone());
    
    // We clone the Arc to the mutex so the background thread can use it
    let tunnel_state_arc = Arc::clone(&state.tunnel);
    let app_clone = app.clone();
    
    tokio::spawn(async move {
        if let Err(e) = run_tunnel_client(
            local_port,
            server_host,
            subdomain,
            cancel_token,
            app_clone.clone(),
            tunnel_state_arc,
        ).await {
            error!("Tunnel error: {}", e);
            emit_log(&app_clone, "error", &format!("Tunnel error: {}", e));
            emit_status(&app_clone, "Offline", None);
        }
    });

    Ok("Tunnel starting...".to_string())
}

#[tauri::command]
async fn stop_tunnel(state: State<'_, AppState>, app: tauri::AppHandle) -> Result<(), String> {
    let mut tunnel = state.tunnel.lock().await;
    
    if let Some(token) = tunnel.cancel_token.take() {
        token.cancel();
        tunnel.public_url = None;
        emit_log(&app, "info", "Tunnel stopped");
        emit_status(&app, "Offline", None);
        Ok(())
    } else {
        Err("Tunnel is not running".to_string())
    }
}

#[tauri::command]
async fn get_status(state: State<'_, AppState>) -> Result<StatusEvent, String> {
    let tunnel = state.tunnel.lock().await;
    Ok(StatusEvent {
        status: if tunnel.cancel_token.is_some() { "Online".to_string() } else { "Offline".to_string() },
        public_url: tunnel.public_url.clone(),
    })
}

// --- Background Logic ---

async fn run_tunnel_client(
    local_port: u16,
    server_host: String,
    subdomain: String,
    cancel_token: CancellationToken,
    app: tauri::AppHandle,
    tunnel_state: Arc<Mutex<TunnelState>>,
) -> Result<(), anyhow::Error> {
    let control_addr = format!("{}:4000", server_host);
    
    loop {
        emit_log(&app, "info", &format!("Connecting to control server at {}", control_addr));
        
        match connect_with_retry(&control_addr, &cancel_token, &app).await {
            Ok(stream) => {
                emit_log(&app, "info", "Connected to control server");
                
                if let Err(e) = handle_control_channel(
                    stream,
                    local_port,
                    &server_host,
                    &subdomain,
                    &cancel_token,
                    &app,
                    &tunnel_state,
                ).await {
                    emit_log(&app, "error", &format!("Control channel error: {}", e));
                }
            }
            Err(e) => {
                emit_log(&app, "error", &format!("Connection failed: {}", e));
            }
        }

        if cancel_token.is_cancelled() {
            break;
        }

        emit_log(&app, "info", "Reconnecting in 5 seconds...");
        tokio::select! {
            _ = cancel_token.cancelled() => break,
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
        }
    }

    Ok(())
}

async fn connect_with_retry(
    addr: &str,
    cancel_token: &CancellationToken,
    app: &tauri::AppHandle,
) -> Result<TcpStream, anyhow::Error> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                if attempts >= 3 { return Err(e.into()); }
                emit_log(app, "warn", &format!("Connection attempt {} failed, retrying...", attempts));
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
        if cancel_token.is_cancelled() {
            return Err(anyhow::anyhow!("Cancelled"));
        }
    }
}

async fn handle_control_channel(
    mut stream: TcpStream,
    local_port: u16,
    server_host: &str,
    subdomain: &str,
    cancel_token: &CancellationToken,
    app: &tauri::AppHandle,
    tunnel_state: &Arc<Mutex<TunnelState>>,
) -> Result<(), anyhow::Error> {
    let register = ControlMessage::Register { subdomain: subdomain.to_string() };
    stream.write_all(&serde_json::to_vec(&register)?).await?;
    
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 { return Err(anyhow::anyhow!("Connection closed")); }

    let msg: ControlMessage = serde_json::from_slice(&buf[..n])?;
    if let ControlMessage::RegisterAck { public_url, .. } = msg {
        emit_log(app, "info", &format!("Registered! URL: {}", public_url));
        emit_status(app, "Online", Some(&public_url));
        let mut ts = tunnel_state.lock().await;
        ts.public_url = Some(public_url);
    } else {
        return Err(anyhow::anyhow!("Unexpected registration response"));
    };

    loop {
        let mut buf = vec![0u8; 4096];
        tokio::select! {
            _ = cancel_token.cancelled() => break,
            result = stream.read(&mut buf) => {
                match result {
                    Ok(0) => break,
                    Ok(n) => {
                        if let Ok(ControlMessage::NewConnection { tunnel_id }) = serde_json::from_slice(&buf[..n]) {
                            emit_log(app, "info", &format!("New connection: {}", tunnel_id));
                            emit_connection(app, &tunnel_id);
                            
                            let app_clone = app.clone();
                            let s_host = server_host.to_string();
                            tokio::spawn(async move {
                                let _ = handle_tunnel_connection(local_port, &s_host, &tunnel_id, &app_clone).await;
                            });
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    Ok(())
}

async fn handle_tunnel_connection(
    local_port: u16,
    server_host: &str,
    tunnel_id: &str,
    app: &tauri::AppHandle,
) -> Result<(), anyhow::Error> {
    let mut data_stream = TcpStream::connect(format!("{}:4001", server_host)).await?;
    let ack = ControlMessage::ConnectionAck { tunnel_id: tunnel_id.to_string() };
    data_stream.write_all(&serde_json::to_vec(&ack)?).await?;

    let mut local_stream = TcpStream::connect(format!("127.0.0.1:{}", local_port)).await?;
    let _ = tokio::io::copy_bidirectional(&mut data_stream, &mut local_stream).await;
    Ok(())
}

// --- Event Emitters ---

fn emit_log(app: &tauri::AppHandle, level: &str, message: &str) {
    let event = LogEvent {
        level: level.to_string(),
        message: message.to_string(),
        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
    };
    let _ = app.emit("log", event);
}

fn emit_status(app: &tauri::AppHandle, status: &str, public_url: Option<&str>) {
    let event = StatusEvent {
        status: status.to_string(),
        public_url: public_url.map(|s| s.to_string()),
    };
    let _ = app.emit("status", event);
}

fn emit_connection(app: &tauri::AppHandle, tunnel_id: &str) {
    let event = ConnectionEvent {
        tunnel_id: tunnel_id.to_string(),
        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
    };
    let _ = app.emit("connection", event);
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .setup(|app| {
            app.manage(AppState {
                tunnel: Arc::new(Mutex::new(TunnelState {
                    cancel_token: None,
                    public_url: None,
                })),
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![start_tunnel, stop_tunnel, get_status])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}