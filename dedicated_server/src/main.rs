mod resources;

use bytes::Bytes;
use resources::{ServerConfig, PlayerRegistry};
use shared::Heartbeat;
use tokio::time::{interval, Duration};
use game_sockets::{GamePeer, GameNetworkEvent, GameStreamReliability, GameConnection, GameStream};
use game_sockets::protocols::QuicBackend;

#[tokio::main]
async fn main() {
    let config = ServerConfig::from_env();
    let mut registry = PlayerRegistry::new();

    println!("[INFO] Dedicated Game Server started");
    println!("[INFO] Server ID: {}", config.id);
    println!("[INFO] Listening on: 0.0.0.0:{}", config.port);
    println!("[INFO] Zone: {}", config.zone);
    println!("[INFO] Max players: {}", config.max_players);
    println!("[INFO] Orchestrator address: {}", config.orchestrator_addr);

    // Bind Quic backend
    let mut peer = GamePeer::new(QuicBackend::new());
    if let Err(e) = peer.listen("0.0.0.0", config.port) {
        eprintln!("[ERROR] Failed to bind Quic: {}", e);
        return;
    }

    // Connect to orchestrator for heartbeat via QUIC
    if let Err(e) = peer.connect(&config.orchestrator_addr.ip().to_string(), config.orchestrator_addr.port()) {
        eprintln!("[ERROR] Failed to connect to orchestrator: {}", e);
        return;
    }

    let mut interval_timer = interval(Duration::from_secs(5));
    let mut orchestrator_conn: Option<GameConnection> = None;
    let orchestrator_stream = GameStream::new(0, GameStreamReliability::Unreliable);

    loop {
        tokio::select! {
            _ = interval_timer.tick() => {
                let heartbeat = Heartbeat {
                    id: config.id.clone(),
                    ip: "0.0.0.0".to_string(),
                    port: config.port,
                    zone: config.zone.clone(),
                    player_count: registry.get_player_count(),
                    max_players: config.max_players,
                };

                if let Some(conn) = &orchestrator_conn {
                    if let Ok(json) = serde_json::to_string(&heartbeat) {
                        match peer.send(conn, &orchestrator_stream, Bytes::from(json)) {
                            Ok(_) => {
                                let status = if registry.is_full(config.max_players) { "full" } else { "available" };
                                println!("[INFO] Heartbeat sent via QUIC - Players: {}/{}, Status: {}",
                                         heartbeat.player_count, heartbeat.max_players, status);
                            }
                            Err(e) => {
                                eprintln!("[WARN] Failed to send heartbeat via QUIC: {}", e);
                            }
                        }
                    }
                } else {
                    println!("[WARN] Waiting for orchestrator connection to send heartbeat...");
                }
            }
            // Add a small sleep to not spin loop if peer.poll is empty
            _ = tokio::time::sleep(Duration::from_millis(1)) => {
                while let Ok(Some(event)) = peer.poll() {
                    match event {
                        GameNetworkEvent::Connected(conn) => {
                            if orchestrator_conn.is_none() {
                                println!("[INFO] Orchestrator connected: {:?}", conn);
                                orchestrator_conn = Some(conn);
                            } else {
                                println!("[INFO] Peer connected: {:?}", conn);
                            }
                        }
                        GameNetworkEvent::Disconnected(conn) => {
                            println!("[INFO] Peer disconnected: {:?}", conn);
                            registry.remove_player(&conn);
                            if Some(conn) == orchestrator_conn {
                                orchestrator_conn = None;
                                println!("[WARN] Orchestrator disconnected.");
                            }
                        }
                        GameNetworkEvent::Message { connection, stream, data } => {
                            let message = String::from_utf8_lossy(&data);
                            println!("[DEBUG] Received message from {:?} on stream {:?}: {}", connection, stream, message);

                            // Parse message: "JOIN username"
                            if let Some(username) = message.strip_prefix("JOIN ") {
                                let username = username.trim().to_string();

                                // Verify if server is not full
                                if !registry.is_full(config.max_players) {
                                    let player = registry.add_player(connection.clone(), username.clone());
                                    println!("[INFO] Player {} connected with ID: {}", username, player.id);

                                    // Send WELCOME response
                                    let response = format!("WELCOME {}", player.id);
                                    let _ = peer.send(&connection, &stream, Bytes::from(response));
                                    println!("[DEBUG] Sent WELCOME response");
                                } else {
                                    println!("[WARN] Server is full, rejected connection ({})", username);
                                    let response = "FULL";
                                    let _ = peer.send(&connection, &stream, Bytes::from(response));
                                }
                            }
                        }
                        GameNetworkEvent::Error { connection, inner } => {
                            eprintln!("[ERROR] Network error for {:?}: {}", connection, inner);
                        }
                        GameNetworkEvent::StreamCreated(conn, stream) => {
                             println!("[INFO] Stream created for {:?}: {:?}", conn, stream);
                        }
                        GameNetworkEvent::StreamClosed(conn, stream) => {
                             println!("[INFO] Stream closed for {:?}: {:?}", conn, stream);
                        }
                    }
                }
            }
        }
    }
}