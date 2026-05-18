use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use shared::ServerInfo;

use crate::redis_pool::find_available_server;
use crate::AppState;

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub player_id: String,
    pub server: ServerInfo
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String
}

pub async fn health_handler() ->impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

pub async fn login_handler(State(state): State<Arc<AppState>>, Json(payload): Json<LoginRequest>) -> impl IntoResponse {
    if payload.username.trim().is_empty() || payload.password != "1234" {
        let error = ErrorResponse {
            error: "Unauthorized".to_string()
        };
        return (StatusCode::UNAUTHORIZED, Json(error)).into_response();
    }

    match find_available_server(&state.redis_pool).await {
        Some(server_info) => {
            let response = LoginResponse {
                player_id: Uuid::new_v4().to_string(),
                server: server_info
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        None => {
            let error = ErrorResponse {
                error: "No server available".to_string()
            };
            (StatusCode::SERVICE_UNAVAILABLE, Json(error)).into_response()
        }
    }
}