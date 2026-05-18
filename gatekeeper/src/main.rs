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

    let shared_state = Arc::new(AppState {redis_pool: pool});

    let app = Router::new()
        .route("/health", get(handlers::health_handler))
        .route("/login", post(handlers::login_handler))
        .with_state(shared_state);

    let addr = "0.0.0.0:3000";
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Gatekeeper listening on {}", addr);

    axum::serve(listener, app).await.unwrap();
}