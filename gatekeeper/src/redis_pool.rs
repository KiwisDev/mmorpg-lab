use deadpool_redis::{Config, Pool, Runtime};
use redis::AsyncCommands;
use shared::ServerInfo;

pub fn create_pool(redis_url: &str) -> Result<Pool, deadpool_redis::CreatePoolError> {
    let config = Config::from_url(redis_url);
    config.create_pool(Some(Runtime::Tokio1))
}

pub async fn find_available_server(pool: &Pool) -> Option<ServerInfo> {
    let mut connection = pool.get().await.ok()?;

    // Get all server stored in redis
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("server:*")
        .query_async(&mut *connection)
        .await
        .ok()?;

    // Return the first available server
    for key in keys {
        let (status, ip, port, zone): (Option<String>, Option<String>, Option<u16>, Option<String>) =
            redis::cmd("HMGET")
                .arg(&key)
                .arg("status")
                .arg("ip")
                .arg("port")
                .arg("zone")
                .query_async(&mut *connection)
                .await
                .unwrap_or((None, None, None, None));

        if let Some(s) = status {
            if s == "available" {
                if let (Some(ip), Some(port), Some(zone)) = (ip, port, zone) {
                    return Some(ServerInfo { ip, port, zone });
                }
            }
        }
    }

    None
}