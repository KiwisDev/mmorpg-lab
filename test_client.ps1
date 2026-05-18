# Rust code to simulate a simple QUIC client to test the dedicated server
# Since QUIC isn't natively supported by basic powershell tools (like netcat/telnet for TCP/UDP)
# We will create a small temporary rust project to act as the client.

$ClientDir = "D:\devoirs\Multi\mmorpg-lab\temp_quic_client"

if (Test-Path $ClientDir) {
    Remove-Item -Recurse -Force $ClientDir
}

cargo new $ClientDir
Set-Location $ClientDir

# Add dependencies
cargo add tokio -F full
cargo add bytes

# Link the local game_sockets library
Add-Content -Path Cargo.toml -Value "`ngame_sockets = { path = `"../game_sockets`" }"

$ClientCode = @"
use game_sockets::{GamePeer, GameNetworkEvent, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use bytes::Bytes;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    println!("Starting test client...");
    let mut peer = GamePeer::new(QuicBackend::new());

    // Connect to the dedicated server
    println!("Connecting to 127.0.0.1:9000...");
    if let Err(e) = peer.connect("127.0.0.1", 9000) {
        eprintln!("Failed to connect: {}", e);
        return;
    }

    // Give it a moment to connect
    sleep(Duration::from_millis(500)).await;

    // Send a JOIN message once connected
    let mut connected_server = None;

    loop {
        while let Ok(Some(event)) = peer.poll() {
            match event {
                GameNetworkEvent::Connected(conn) => {
                    println!("Successfully connected!");
                    connected_server = Some(conn);

                    // Send join message
                    if let Some(c) = &connected_server {
                        // Create an unreliable stream for simple messages like JOIN
                        peer.create_stream(*c, GameStreamReliability::Unreliable).unwrap();
                    }
                }
                GameNetworkEvent::StreamCreated(conn, stream) => {
                    println!("Stream created. Sending JOIN message...");
                    peer.send(&conn, &stream, Bytes::from("JOIN Player1")).unwrap();
                }
                GameNetworkEvent::Message { connection: _, stream: _, data } => {
                    let msg = String::from_utf8_lossy(&data);
                    println!("Received from server: {}", msg);
                    if msg.starts_with("WELCOME") || msg == "FULL" {
                        println!("Test completed. Exiting.");
                        return;
                    }
                }
                GameNetworkEvent::Error { connection: _, inner } => {
                    eprintln!("Error: {}", inner);
                }
                _ => {}
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
}
"@

Set-Content -Path src\main.rs -Value $ClientCode
Write-Host "Building and running the test client..."
cargo run