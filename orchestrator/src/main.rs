use std::time::Duration;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use redis::AsyncCommands;
use shared::Heartbeat;

const HEARTBEAT_TTL_SECONDS: u64 = 15;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("ORCH_PORT")
        .unwrap_or("9000".to_string())
        .parse()
        .expect("ORCH_PORT must be a valid port number");

    let redis_url = std::env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".to_string());

    // Open a persistent async connection to Redis.
    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect to Redis");

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

async fn register_server(redis_conn: &mut redis::aio::MultiplexedConnection, heartbeat: &Heartbeat) {
    let key = format!("server:{}", heartbeat.id);

    let status = if heartbeat.player_count >= heartbeat.max_players {
        "full"
    } else {
        "available"
    };

    // HSET writes multiple fields at once into a Redis hash.
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
