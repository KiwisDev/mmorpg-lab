use std::time::Duration;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use shared::Heartbeat;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("ORCH_PORT")
        .unwrap_or("9000".to_string())
        .parse()
        .expect("ORCH_PORT must be a valid port number");

    let mut peer = GamePeer::new(QuicBackend::new());
    peer.listen("0.0.0.0", port).expect("Failed to bind QUIC socket");
    println!("Orchestrator listening on port {}", port);

    loop {
        match peer.poll() {
            Ok(Some(GameNetworkEvent::Connected(conn))) => {
                println!("Server connected: {}", conn.connection_id);
            }
            Ok(Some(GameNetworkEvent::Disconnected(conn))) => {
                println!("Server disconnected: {}", conn.connection_id);
            }
            Ok(Some(GameNetworkEvent::Message { data, .. })) => {
                match serde_json::from_slice::<Heartbeat>(&data) {
                    Ok(heartbeat) => {
                        println!(
                            "Heartbeat from server {} (zone: {}, players: {}/{})",
                            heartbeat.id, heartbeat.zone, heartbeat.player_count, heartbeat.max_players
                        );
                    }
                    Err(_) => {
                        println!("Unknown message: {:?}", data);
                    }
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                // No event yet — yield briefly to avoid busy-waiting.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => {
                eprintln!("Network error: {}", e);
            }
        }
    }
}