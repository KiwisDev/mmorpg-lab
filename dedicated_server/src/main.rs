mod resources;

use resources::{ServerConfig, PlayerRegistry};
use shared::Heartbeat;
use tokio::net::UdpSocket;
use tokio::time::{interval, Duration};

#[tokio::main]
async fn main() {
    let config = ServerConfig::from_env();
    let mut registry = PlayerRegistry::new();

    println!("[INFO] Dedicated Game Server started");
    println!("[INFO] Server ID: {}", config.id);
    println!("[INFO] Listening on: 127.0.0.1:{}", config.port);
    println!("[INFO] Zone: {}", config.zone);
    println!("[INFO] Max players: {}", config.max_players);
    println!("[INFO] Orchestrator address: {}", config.orchestrator_addr);

    // Bind UDP socket
    let addr = format!("127.0.0.1:{}", config.port);
    let socket = UdpSocket::bind(&addr)
        .await
        .expect(&format!("Failed to bind socket on {}", addr));

    // Single-threaded event loop using tokio::select! to handle incoming packets and periodic heartbeats
    let mut interval_timer = interval(Duration::from_secs(5));
    let mut buf = [0u8; 1024];

    loop {
        tokio::select! {
            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok((size, addr)) => {
                        let message = String::from_utf8_lossy(&buf[..size]);
                        println!("[DEBUG] Received message from {}: {}", addr, message);

                        // Parse message: "JOIN username"
                        if let Some(username) = message.strip_prefix("JOIN ") {
                            let username = username.trim().to_string();

                            // Verify if server is not full
                            if !registry.is_full(config.max_players) {
                                let player = registry.add_player(addr, username.clone());
                                println!("[INFO] Player {} connected from {} with ID: {}", username, addr, player.id);

                                // Send WELCOME response
                                let response = format!("WELCOME {}", player.id);
                                let _ = socket.send_to(response.as_bytes(), addr).await;
                                println!("[DEBUG] Sent WELCOME response to {}", addr);
                            } else {
                                println!("[WARN] Server is full, rejected connection from {} ({})", addr, username);
                                let response = "FULL";
                                let _ = socket.send_to(response.as_bytes(), addr).await;
                            }
                        }
                    }
                    Err(e) => {
                        // Ignore Windows error 10054 (ICMP port unreachable from heartbeat)
                        if e.raw_os_error() == Some(10054) {
                            // Silent continue - this is expected behavior when orchestrator is not listening
                            continue;
                        }
                        eprintln!("[ERROR] Error receiving packet: {}", e);
                        break;
                    }
                }
            }
            _ = interval_timer.tick() => {
                let heartbeat = Heartbeat {
                    id: config.id.clone(),
                    ip: "127.0.0.1".to_string(),
                    port: config.port,
                    zone: config.zone.clone(),
                    player_count: registry.get_player_count(),
                    max_players: config.max_players,
                };

                if let Ok(json) = serde_json::to_string(&heartbeat) {
                    match socket.send_to(json.as_bytes(), config.orchestrator_addr).await {
                        Ok(_) => {
                            let status = if registry.is_full(config.max_players) { "FULL" } else { "AVAILABLE" };
                            println!("[INFO] Heartbeat sent - Players: {}/{}, Status: {}",
                                     heartbeat.player_count, heartbeat.max_players, status);
                        }
                        Err(e) => {
                            eprintln!("[WARN] Failed to send heartbeat: {} (orchestrator may not be listening)", e.kind());
                        }
                    }
                }
            }
        }
    }
}