use serde::{Deserialize, Serialize};

use crate::{
    error::AppError,
    models::redis::{RedisConfig, ServerConfig},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRequest {
    pub source: RedisConfig,
    pub target: RedisConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushDbRequest {
    pub target: RedisConfig,
    pub confirm_text: String,
}

impl FlushDbRequest {
    pub fn validate_confirm(&self) -> Result<(), AppError> {
        let expected = format!("FLUSHDB db={} host={}", self.target.db, self.target.host);
        if self.confirm_text != expected {
            return Err(AppError::BadRequest(format!(
                "invalid confirm_text, expected: {}",
                expected
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteKeysRequest {
    pub target: RedisConfig,
    pub keys: Vec<String>,
    pub confirm_text: String,
}

impl DeleteKeysRequest {
    pub fn validate_confirm(&self) -> Result<(), AppError> {
        let expected = format!("DELETE {} db={}", self.keys.len(), self.target.db);
        if self.confirm_text != expected {
            return Err(AppError::BadRequest(format!(
                "invalid confirm_text, expected: {}",
                expected
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDeleteRequest {
    pub target: RedisConfig,
    pub tables: Vec<String>,
    pub confirm_text: String,
}

impl TableDeleteRequest {
    pub fn validate_confirm(&self) -> Result<(), AppError> {
        let expected = format!("DELETE_TABLES {} db={}", self.tables.len(), self.target.db);
        if self.confirm_text != expected {
            return Err(AppError::BadRequest(format!(
                "invalid confirm_text, expected: {}",
                expected
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashGetRequest {
    pub target: RedisConfig,
    pub hash_name: String,
    pub field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashListRequest {
    pub target: RedisConfig,
    pub hash_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashSetRequest {
    pub target: RedisConfig,
    pub hash_name: String,
    pub field: String,
    pub base64_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalizeAccountRequest {
    pub source: RedisConfig,
    pub target: RedisConfig,
    pub hash_name: String,
    pub source_field: String,
    pub target_field: Option<String>,
    pub server: ServerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchLocalizeRequest {
    pub source: RedisConfig,
    pub target: RedisConfig,
    pub hash_name: String,
    pub source_fields: Vec<String>,
    pub server: ServerConfig,
}
