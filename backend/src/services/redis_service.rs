use std::collections::HashMap;

use anyhow::Context;
use base64::{Engine as _, engine::general_purpose};
use redis::{AsyncCommands, aio::MultiplexedConnection};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{models::redis::RedisConfig, utils::msgpack};

#[derive(Debug, Serialize, Deserialize)]
pub struct HashVisualizedResult {
    pub hash_name: String,
    pub field: String,
    pub raw_base64: String,
    pub raw_size: usize,
    pub decoded_type: String,
    pub decoded_json: Option<Value>,
    pub decoded_text: Option<String>,
}

pub async fn create_connection(cfg: &RedisConfig) -> anyhow::Result<MultiplexedConnection> {
    let url = if let Some(pass) = &cfg.password {
        format!("redis://:{}@{}:{}/{}", pass, cfg.host, cfg.port, cfg.db)
    } else {
        format!("redis://{}:{}/{}", cfg.host, cfg.port, cfg.db)
    };

    let client = redis::Client::open(url).context("failed to create redis client")?;
    let conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect redis")?;
    Ok(conn)
}

pub async fn test_connection(cfg: &RedisConfig) -> anyhow::Result<()> {
    let mut conn = create_connection(cfg).await?;
    let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
    if pong != "PONG" {
        anyhow::bail!("unexpected ping response: {}", pong);
    }
    Ok(())
}

pub async fn flushdb(cfg: &RedisConfig) -> anyhow::Result<()> {
    let mut conn = create_connection(cfg).await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await?;
    Ok(())
}

pub async fn delete_keys(cfg: &RedisConfig, keys: &[String]) -> anyhow::Result<usize> {
    if keys.is_empty() {
        return Ok(0);
    }
    let mut conn = create_connection(cfg).await?;
    let deleted: usize = redis::cmd("DEL").arg(keys).query_async(&mut conn).await?;
    Ok(deleted)
}

pub async fn delete_tables(cfg: &RedisConfig, tables: &[String]) -> anyhow::Result<usize> {
    if tables.is_empty() {
        return Ok(0);
    }

    let mut conn = create_connection(cfg).await?;
    let mut deleted = 0usize;

    for table in tables {
        let n: usize = redis::cmd("DEL").arg(table).query_async(&mut conn).await?;
        deleted += n;
    }

    Ok(deleted)
}

pub async fn backup_db(source: &RedisConfig, target: &RedisConfig) -> anyhow::Result<usize> {
    let mut src = create_connection(source).await?;
    let mut dst = create_connection(target).await?;

    let _: () = redis::cmd("FLUSHDB").query_async(&mut dst).await?;

    let keys: Vec<String> = redis::cmd("KEYS").arg("*").query_async(&mut src).await?;
    let mut copied = 0usize;

    for key in keys {
        let dumped: Vec<u8> = redis::cmd("DUMP").arg(&key).query_async(&mut src).await?;
        let _: () = redis::cmd("RESTORE")
            .arg(&key)
            .arg(0)
            .arg(dumped)
            .arg("REPLACE")
            .query_async(&mut dst)
            .await?;
        copied += 1;
    }

    Ok(copied)
}

pub async fn list_hash_fields(cfg: &RedisConfig, hash_name: &str) -> anyhow::Result<Vec<String>> {
    let mut conn = create_connection(cfg).await?;
    list_hash_fields_with_conn(&mut conn, hash_name).await
}

pub async fn list_hash_fields_with_conn(
    conn: &mut MultiplexedConnection,
    hash_name: &str,
) -> anyhow::Result<Vec<String>> {
    let fields: Vec<String> = redis::cmd("HKEYS").arg(hash_name).query_async(conn).await?;
    Ok(fields)
}

pub async fn scan_hash_entries(
    conn: &mut MultiplexedConnection,
    hash_name: &str,
    cursor: u64,
    count: usize,
) -> anyhow::Result<(u64, Vec<(String, Vec<u8>)>)> {
    let result: (u64, Vec<(String, Vec<u8>)>) = redis::cmd("HSCAN")
        .arg(hash_name)
        .arg(cursor)
        .arg("COUNT")
        .arg(count)
        .query_async(conn)
        .await
        .with_context(|| {
            format!(
                "failed to hscan hash {}, cursor={}, count={}",
                hash_name, cursor, count
            )
        })?;

    Ok(result)
}

pub async fn get_hash_field_bytes(
    cfg: &RedisConfig,
    hash_name: &str,
    field: &str,
) -> anyhow::Result<Vec<u8>> {
    let mut conn = create_connection(cfg).await?;
    get_hash_field_bytes_with_conn(&mut conn, hash_name, field).await
}

pub async fn get_hash_field_bytes_with_conn(
    conn: &mut MultiplexedConnection,
    hash_name: &str,
    field: &str,
) -> anyhow::Result<Vec<u8>> {
    let value: Option<Vec<u8>> = conn.hget(hash_name, field).await?;
    match value {
        Some(v) => Ok(v),
        None => anyhow::bail!("field not found: {} in hash {}", field, hash_name),
    }
}

pub async fn get_hash_fields_bytes_with_conn(
    conn: &mut MultiplexedConnection,
    hash_name: &str,
    fields: &[String],
) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
    if fields.is_empty() {
        return Ok(Vec::new());
    }

    let values: Vec<Option<Vec<u8>>> = redis::cmd("HMGET")
        .arg(hash_name)
        .arg(fields)
        .query_async(conn)
        .await
        .with_context(|| {
            format!(
                "failed to hmget {} fields from hash {}",
                fields.len(),
                hash_name
            )
        })?;

    Ok(values)
}

pub async fn set_hash_field_bytes(
    cfg: &RedisConfig,
    hash_name: &str,
    field: &str,
    value: Vec<u8>,
) -> anyhow::Result<()> {
    let mut conn = create_connection(cfg).await?;
    set_hash_field_bytes_with_conn(&mut conn, hash_name, field, value).await
}

pub async fn set_hash_field_bytes_with_conn(
    conn: &mut MultiplexedConnection,
    hash_name: &str,
    field: &str,
    value: Vec<u8>,
) -> anyhow::Result<()> {
    let _: () = conn.hset(hash_name, field, value).await?;
    Ok(())
}

pub async fn set_hash_fields_bytes_pipeline(
    conn: &mut MultiplexedConnection,
    hash_name: &str,
    items: &[(String, Vec<u8>)],
) -> anyhow::Result<()> {
    if items.is_empty() {
        return Ok(());
    }

    // HSET supports multiple field/value pairs in one command. This avoids sending
    // one command per field and is noticeably faster for full-table localization.
    let mut cmd = redis::cmd("HSET");
    cmd.arg(hash_name);

    for (field, value) in items {
        cmd.arg(field).arg(value);
    }

    let _: usize = cmd.query_async(conn).await?;
    Ok(())
}

pub async fn get_hash_field_raw(
    cfg: &RedisConfig,
    hash_name: &str,
    field: &str,
) -> anyhow::Result<String> {
    let bytes = get_hash_field_bytes(cfg, hash_name, field).await?;
    Ok(general_purpose::STANDARD.encode(bytes))
}

pub async fn set_hash_field_raw(
    cfg: &RedisConfig,
    hash_name: &str,
    field: &str,
    base64_value: &str,
) -> anyhow::Result<()> {
    let bytes = general_purpose::STANDARD
        .decode(base64_value)
        .context("invalid base64_value")?;
    set_hash_field_bytes(cfg, hash_name, field, bytes).await
}

pub async fn get_hash_field_visualized(
    cfg: &RedisConfig,
    hash_name: &str,
    field: &str,
) -> anyhow::Result<HashVisualizedResult> {
    let bytes = get_hash_field_bytes(cfg, hash_name, field).await?;
    let raw_base64 = general_purpose::STANDARD.encode(&bytes);

    if let Ok(json_value) = msgpack::decode_to_json_value(&bytes) {
        return Ok(HashVisualizedResult {
            hash_name: hash_name.to_string(),
            field: field.to_string(),
            raw_base64,
            raw_size: bytes.len(),
            decoded_type: "msgpack-json".to_string(),
            decoded_json: Some(json_value),
            decoded_text: None,
        });
    }

    if let Ok(text) = String::from_utf8(bytes.clone()) {
        return Ok(HashVisualizedResult {
            hash_name: hash_name.to_string(),
            field: field.to_string(),
            raw_base64,
            raw_size: bytes.len(),
            decoded_type: "utf8-text".to_string(),
            decoded_json: None,
            decoded_text: Some(text),
        });
    }

    Ok(HashVisualizedResult {
        hash_name: hash_name.to_string(),
        field: field.to_string(),
        raw_base64,
        raw_size: bytes.len(),
        decoded_type: "binary".to_string(),
        decoded_json: None,
        decoded_text: None,
    })
}

pub async fn get_hash_all_as_map(
    cfg: &RedisConfig,
    hash_name: &str,
) -> anyhow::Result<HashMap<String, Vec<u8>>> {
    let mut conn = create_connection(cfg).await?;
    let map: HashMap<String, Vec<u8>> = conn.hgetall(hash_name).await?;
    Ok(map)
}
