use axum::{Json, Router, routing::post};

use crate::{
    error::AppError,
    models::{
        request::{BatchLocalizeRequest, LocalizeAccountRequest},
        response::ApiResponse,
    },
    services::process_service::{self, LocalizeSummary},
};

pub fn routes() -> Router {
    Router::new()
        .route("/api/process/localize-account", post(localize_account))
        .route("/api/process/localize-batch", post(localize_batch))
        .route("/api/process/localize-all-acc", post(localize_all_acc))
}

async fn localize_account(
    Json(req): Json<LocalizeAccountRequest>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    let target = process_service::localize_single_account(&req).await?;
    Ok(Json(ApiResponse::ok_with_message(
        target,
        "Localized single account successfully".to_string(),
    )))
}

async fn localize_batch(
    Json(req): Json<BatchLocalizeRequest>,
) -> Result<Json<ApiResponse<LocalizeSummary>>, AppError> {
    let summary = process_service::localize_batch(&req).await?;
    Ok(Json(ApiResponse::ok_with_message(
        summary,
        "Localized batch successfully".to_string(),
    )))
}

async fn localize_all_acc(
    Json(req): Json<BatchLocalizeRequest>,
) -> Result<Json<ApiResponse<LocalizeSummary>>, AppError> {
    let summary = process_service::localize_all_acc(&req).await?;
    Ok(Json(ApiResponse::ok_with_message(
        summary,
        "Localized all fields successfully".to_string(),
    )))
}
