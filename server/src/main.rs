use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

const CONTROL_PORT: u16 = 4000;
const DATA_PORT: u16 = 4001;
const PUBLIC_PORT: u16 = 8080;

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

#[derive(Clone)]
struct ClientInfo {
    subdomain: String,
    control_tx: mpsc::Sender<ControlMessage>,
}

type ClientRegistry = Arc<DashMap<String, ClientInfo>>;
type PendingConnections = Arc<DashMap<String, TcpStream>>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    info!("Starting Reverse Tunnel Server");
    info!("Control Port: {}", CONTROL_PORT);
    info!("Data Port: {}", DATA_PORT);
    info!("Public Port: {}", PUBLIC_PORT);

    let clients: ClientRegistry = Arc::new(DashMap::new());
    let pending: PendingConnections = Arc::new(DashMap::new());
    let cancel_token = CancellationToken::new();

    let control_handle = {
        let clients = clients.clone();
        let token = cancel_token.clone();
        tokio::spawn(async move {
            if let Err(e) = run_control_server(clients, token).await {
                error!("Control server error: {}", e);
            }
        })
    };

    let data_handle = {
        let clients = clients.clone();
        let pending = pending.clone();
        let token = cancel_token.clone();
        tokio::spawn(async move {
            if let Err(e) = run_data_server(clients, pending, token).await {
                error!("Data server error: {}", e);
            }
        })
    };

    let public_handle = {
        let clients = clients.clone();
        let pending = pending.clone();
        let token = cancel_token.clone();
        tokio::spawn(async move {
            if let Err(e) = run_public_server(clients, pending, token).await {
                error!("Public server error: {}", e);
            }
        })
    };

    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    cancel_token.cancel();

    let _ = tokio::join!(control_handle, data_handle, public_handle);
    info!("Server stopped");
    Ok(())
}

async fn run_control_server(clients: ClientRegistry, token: CancellationToken) -> Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", CONTROL_PORT))
        .await
        .context("Failed to bind control port")?;
    info!("Control server listening on port {}", CONTROL_PORT);

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        info!("New control connection from {}", addr);
                        let clients = clients.clone();
                        let token = token.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_control_connection(stream, clients, token).await {
                                error!("Control connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => error!("Accept error: {}", e),
                }
            }
        }
    }
    Ok(())
}

async fn handle_control_connection(
    mut stream: TcpStream,
    clients: ClientRegistry,
    token: CancellationToken,
) -> Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    
    if n == 0 {
        return Ok(());
    }

    let msg: ControlMessage = serde_json::from_slice(&buf[..n])?;
    
    let subdomain = match msg {
        ControlMessage::Register { subdomain } => subdomain,
        _ => anyhow::bail!("Expected Register message"),
    };

    info!("Client registered with subdomain: {}", subdomain);
    
    let public_url = format!("http://localhost:{}/{}", PUBLIC_PORT, subdomain);
    let (tx, mut rx) = mpsc::channel::<ControlMessage>(100);
    
    clients.insert(subdomain.clone(), ClientInfo {
        subdomain: subdomain.clone(),
        control_tx: tx,
    });

    let ack = ControlMessage::RegisterAck {
        subdomain: subdomain.clone(),
        public_url: public_url.clone(),
    };
    let ack_bytes = serde_json::to_vec(&ack)?;
    stream.write_all(&ack_bytes).await?;
    stream.write_all(b"\n").await?;

    info!("Client {} registered, public URL: {}", subdomain, public_url);

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            msg = rx.recv() => {
                match msg {
                    Some(msg) => {
                        let bytes = serde_json::to_vec(&msg)?;
                        if let Err(e) = stream.write_all(&bytes).await {
                            error!("Failed to send message: {}", e);
                            break;
                        }
                        if let Err(e) = stream.write_all(b"\n").await {
                            error!("Failed to send newline: {}", e);
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    clients.remove(&subdomain);
    info!("Client {} disconnected", subdomain);
    Ok(())
}

async fn run_data_server(
    _clients: ClientRegistry,
    pending: PendingConnections,
    token: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", DATA_PORT))
        .await
        .context("Failed to bind data port")?;
    info!("Data server listening on port {}", DATA_PORT);

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        info!("New data connection from {}", addr);
                        let pending = pending.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_data_connection(stream, pending).await {
                                error!("Data connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => error!("Accept error: {}", e),
                }
            }
        }
    }
    Ok(())
}

async fn handle_data_connection(mut stream: TcpStream, pending: PendingConnections) -> Result<()> {
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    
    if n == 0 {
        return Ok(());
    }

    let msg: ControlMessage = serde_json::from_slice(&buf[..n])?;
    
    let tunnel_id = match msg {
        ControlMessage::ConnectionAck { tunnel_id } => tunnel_id,
        _ => anyhow::bail!("Expected ConnectionAck message"),
    };

    info!("Data connection acknowledged for tunnel {}", tunnel_id);

    if let Some((_, mut public_stream)) = pending.remove(&tunnel_id) {
        info!("Bridging tunnel {}", tunnel_id);

        let result = tokio::io::copy_bidirectional(&mut public_stream, &mut stream).await;
        
        match result {
            Ok((to_client, to_server)) => {
                info!("Tunnel {} closed: {}B sent, {}B received", tunnel_id, to_client, to_server);
            }
            Err(e) => {
                error!("Tunnel {} error: {}", tunnel_id, e);
            }
        }
    } else {
        warn!("No pending connection found for tunnel {}", tunnel_id);
    }

    Ok(())
}

async fn run_public_server(
    clients: ClientRegistry,
    pending: PendingConnections,
    token: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", PUBLIC_PORT))
        .await
        .context("Failed to bind public port")?;
    info!("Public server listening on port {}", PUBLIC_PORT);

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        info!("New public connection from {}", addr);
                        let clients = clients.clone();
                        let pending = pending.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_public_connection(stream, clients, pending).await {
                                error!("Public connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => error!("Accept error: {}", e),
                }
            }
        }
    }
    Ok(())
}

async fn handle_public_connection(
    stream: TcpStream,
    clients: ClientRegistry,
    pending: PendingConnections,
) -> Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = stream.peek(&mut buf).await?;
    
    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..n]);
    let subdomain = extract_subdomain(&request).unwrap_or("default");
    
    info!("Public request for subdomain: {}", subdomain);

    if let Some(client) = clients.get(subdomain) {
        let tunnel_id = Uuid::new_v4().to_string();
        pending.insert(tunnel_id.clone(), stream);

        let msg = ControlMessage::NewConnection {
            tunnel_id: tunnel_id.clone(),
        };

        if let Err(e) = client.control_tx.send(msg).await {
            error!("Failed to notify client: {}", e);
            pending.remove(&tunnel_id);
            return Err(e.into());
        }

        info!("Notified client {} of new connection {}", subdomain, tunnel_id);
    } else {
        warn!("No client registered for subdomain: {}", subdomain);
        let response = b"HTTP/1.1 404 Not Found\r\nContent-Length: 13\r\n\r\nNo such tunnel";
        let _ = stream.try_write(response);
    }

    Ok(())
}

fn extract_subdomain(request: &str) -> Option<&str> {
    let lines: Vec<&str> = request.lines().collect();
    for line in lines {
        if line.starts_with("GET /") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let path = parts[1].trim_start_matches('/');
                if !path.is_empty() {
                    let subdomain = path.split('/').next()?;
                    return Some(subdomain);
                }
            }
        }
    }
    Some("default")
}