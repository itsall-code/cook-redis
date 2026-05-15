use crate::models::response::ApiResponse;
use axum::{Json, Router, routing::get};

pub fn routes() -> Router {
    Router::new().route("/api/health", get(health))
}

async fn health() -> Json<ApiResponse<String>> {
    Json(ApiResponse::ok("cookredis-rs is running".to_string()))
}
