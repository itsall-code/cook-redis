use axum::{
    Json, Router,
    routing::{get, post},
};

use crate::{
    error::AppError,
    models::{
        redis::RedisConfig,
        request::{
            BackupRequest, DeleteKeysRequest, FlushDbRequest, HashGetRequest, HashListRequest,
            HashSetRequest, TableDeleteRequest,
        },
        response::ApiResponse,
    },
    services::redis_service,
};

pub fn routes() -> Router {
    Router::new()
        .route("/api/redis/test", post(test_connection))
        .route("/api/redis/flushdb", post(flush_db))
        .route("/api/redis/delete-keys", post(delete_keys))
        .route("/api/redis/delete-tables", post(delete_tables))
        .route("/api/redis/backup", post(backup_db))
        .route("/api/redis/hash/get", post(get_hash_field))
        .route("/api/redis/hash/set", post(set_hash_field))
        .route("/api/redis/hash/list", post(list_hash_fields))
        .route("/api/redis/ping", get(ping))
}

async fn ping() -> Json<ApiResponse<String>> {
    Json(ApiResponse::ok("pong".to_string()))
}

async fn test_connection(
    Json(cfg): Json<RedisConfig>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    redis_service::test_connection(&cfg).await?;
    Ok(Json(ApiResponse::ok_with_message(
        "connected".to_string(),
        format!("Connected to redis {}:{} db {}", cfg.host, cfg.port, cfg.db),
    )))
}

async fn flush_db(Json(req): Json<FlushDbRequest>) -> Result<Json<ApiResponse<String>>, AppError> {
    req.validate_confirm()?;
    redis_service::flushdb(&req.target).await?;
    Ok(Json(ApiResponse::ok_with_message(
        "db cleared".to_string(),
        format!("Flushed redis {} db {}", req.target.host, req.target.db),
    )))
}

async fn delete_keys(
    Json(req): Json<DeleteKeysRequest>,
) -> Result<Json<ApiResponse<usize>>, AppError> {
    req.validate_confirm()?;
    let count = redis_service::delete_keys(&req.target, &req.keys).await?;
    Ok(Json(ApiResponse::ok_with_message(
        count,
        format!(
            "Deleted {} keys from {} db {}",
            count, req.target.host, req.target.db
        ),
    )))
}

async fn delete_tables(
    Json(req): Json<TableDeleteRequest>,
) -> Result<Json<ApiResponse<usize>>, AppError> {
    req.validate_confirm()?;
    let count = redis_service::delete_keys(&req.target, &req.tables).await?;
    Ok(Json(ApiResponse::ok_with_message(
        count,
        format!(
            "Deleted {} tables from {} db {}",
            count, req.target.host, req.target.db
        ),
    )))
}

async fn backup_db(Json(req): Json<BackupRequest>) -> Result<Json<ApiResponse<usize>>, AppError> {
    let count = redis_service::backup_db(&req.source, &req.target).await?;
    Ok(Json(ApiResponse::ok_with_message(
        count,
        format!(
            "Backup complete: copied {} keys from {} db {} to {} db {}",
            count, req.source.host, req.source.db, req.target.host, req.target.db
        ),
    )))
}

async fn get_hash_field(
    Json(req): Json<HashGetRequest>,
) -> Result<Json<ApiResponse<redis_service::HashVisualizedResult>>, AppError> {
    let result =
        redis_service::get_hash_field_visualized(&req.target, &req.hash_name, &req.field).await?;

    Ok(Json(ApiResponse::ok_with_message(
        result,
        format!("Loaded field {} from hash {}", req.field, req.hash_name),
    )))
}

async fn set_hash_field(
    Json(req): Json<HashSetRequest>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    redis_service::set_hash_field_raw(&req.target, &req.hash_name, &req.field, &req.base64_value)
        .await?;
    Ok(Json(ApiResponse::ok_with_message(
        "hash field updated".to_string(),
        format!("Updated field {} in hash {}", req.field, req.hash_name),
    )))
}

async fn list_hash_fields(
    Json(req): Json<HashListRequest>,
) -> Result<Json<ApiResponse<Vec<String>>>, AppError> {
    let fields = redis_service::list_hash_fields(&req.target, &req.hash_name).await?;
    Ok(Json(ApiResponse::ok_with_message(
        fields,
        format!("Loaded fields from hash {}", req.hash_name),
    )))
}
