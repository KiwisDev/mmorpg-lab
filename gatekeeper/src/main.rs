use axum::{routing::{get, post}, Router};
use std::sync::Arc;
use tokio::net::TcpListener;
use deadpool_redis::Pool;

mod redis_pool;
mod handlers;

pub struct AppState {
    pub redis_pool: Pool
}
#[tokio::main]
async fn main() {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let pool = redis_pool::create_pool(&redis_url).expect("Error when creating Redis pool");
}