use std::time::Duration;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use redis::AsyncCommands;
use shared::Heartbeat;

const HEARTBEAT_TTL_SECONDS: u64 = 15;
const SCALER_INTERVAL_SECONDS: u64 = 10;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("ORCH_PORT")
        .unwrap_or("9000".to_string())
        .parse()
        .expect("ORCH_PORT must be a valid port number");

    let redis_url = std::env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".to_string());

    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect to Redis");

    // Clone the connection for the scaler — MultiplexedConnection is designed to be shared.
    tokio::spawn(scaler_loop(redis_conn.clone()));

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
                if let Ok(heartbeat) = serde_json::from_slice::<Heartbeat>(&data) {
                    println!(
                        "Heartbeat from server {} (zone: {}, players: {}/{})",
                        heartbeat.id, heartbeat.zone, heartbeat.player_count, heartbeat.max_players
                    );
                    register_server(&mut redis_conn, &heartbeat).await;
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => {
                eprintln!("Network error: {}", e);
            }
        }
    }
}

async fn scaler_loop(mut redis_conn: redis::aio::MultiplexedConnection) {
    let hot_servers_min: usize = std::env::var("HOT_SERVERS_MIN")
        .unwrap_or("1".to_string())
        .parse()
        .unwrap_or(1);

    let mut interval = tokio::time::interval(Duration::from_secs(SCALER_INTERVAL_SECONDS));

    loop {
        interval.tick().await;

        let available = count_available_servers(&mut redis_conn).await;
        println!("[scaler] Available servers: {} (min: {})", available, hot_servers_min);

        for _ in available..hot_servers_min {
            spawn_server();
        }
    }
}

async fn count_available_servers(redis_conn: &mut redis::aio::MultiplexedConnection) -> usize {
    let keys: Vec<String> = redis_conn.keys("server:*").await.unwrap_or_default();

    let mut count = 0;
    for key in &keys {
        let status: Option<String> = redis_conn.hget(key, "status").await.unwrap_or(None);
        if status.as_deref() == Some("available") {
            count += 1;
        }
    }
    count
}

fn find_free_port() -> u16 {
    // Binding to port 0 lets the OS assign a free port; we read it then release the socket.
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").expect("Failed to find free port");
    socket.local_addr().unwrap().port()
}

fn spawn_server() {
    let port = find_free_port();

    // The dedicated_server binary sits next to this binary in the same target directory.
    let mut binary_path = std::env::current_exe().expect("Failed to get current exe path");
    binary_path.pop();
    binary_path.push("dedicated_server");

    let orch_addr = std::env::var("ORCH_ADDR").unwrap_or("127.0.0.1:9000".to_string());

    match std::process::Command::new(&binary_path)
        .env("DS_PORT", port.to_string())
        .env("ORCHESTRATOR_ADDR", orch_addr)
        .spawn()
    {
        Ok(_)  => println!("[scaler] Spawned dedicated_server on port {}", port),
        Err(e) => eprintln!("[scaler] Failed to spawn dedicated_server: {}", e),
    }
}

async fn register_server(redis_conn: &mut redis::aio::MultiplexedConnection, heartbeat: &Heartbeat) {
    let key = format!("server:{}", heartbeat.id);

    let status = if heartbeat.player_count >= heartbeat.max_players {
        "full"
    } else {
        "available"
    };

    let result: redis::RedisResult<()> = redis_conn
        .hset_multiple(&key, &[
            ("ip",           heartbeat.ip.as_str()),
            ("port",         &heartbeat.port.to_string()),
            ("zone",         heartbeat.zone.as_str()),
            ("status",       status),
            ("player_count", &heartbeat.player_count.to_string()),
        ])
        .await;

    if let Err(e) = result {
        eprintln!("Redis HSET error: {}", e);
        return;
    }

    // EXPIRE resets the TTL on every heartbeat. If a server stops sending heartbeats,
    // Redis automatically deletes its key after HEARTBEAT_TTL_SECONDS.
    let result: redis::RedisResult<()> = redis_conn
        .expire(&key, HEARTBEAT_TTL_SECONDS as i64)
        .await;

    if let Err(e) = result {
        eprintln!("Redis EXPIRE error: {}", e);
    }
}