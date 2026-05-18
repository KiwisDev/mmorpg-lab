use shared::Heartbeat;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() {
    let port = std::env::var("ORCH_PORT").unwrap_or("9000".to_string());
    let addr = format!("0.0.0.0:{}", port);

    let socket = UdpSocket::bind(&addr).await.expect("Failed to bind UDP socket");
    println!("Orchestrator listening on {}", addr);

    let mut buf = vec![0u8; 1024];

    loop {
        let (nb_bytes, sender) = socket
            .recv_from(&mut buf)
            .await
            .expect("Failed to receive data");

        match serde_json::from_slice::<Heartbeat>(&buf[..nb_bytes]) {
            Ok(heartbeat) => {
                println!(
                    "Heartbeat from server {} (zone: {}, players: {}/{})",
                    heartbeat.id, heartbeat.zone, heartbeat.player_count, heartbeat.max_players
                );
            }
            Err(_) => {
                // Log unexpected messages so we can debug protocol mismatches.
                let raw = String::from_utf8_lossy(&buf[..nb_bytes]);
                println!("Unknown message from {}: {}", sender, raw);
            }
        }
    }
}