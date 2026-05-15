use std::time::Instant;

use anyhow::Context;
use redis::aio::MultiplexedConnection;
use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Number, Value};

use crate::{
    models::request::{BatchLocalizeRequest, LocalizeAccountRequest},
    services::redis_service,
    utils::msgpack,
};

const HSCAN_COUNT: usize = 500;
const PIPELINE_BATCH_SIZE: usize = 200;
const PROGRESS_LOG_EVERY: usize = 500;

#[derive(Debug, Serialize, Clone)]
pub struct LocalizeSummary {
    pub hash_name: String,
    pub scanned: usize,
    pub localized: usize,
    pub skipped: usize,
    pub written: usize,
    pub elapsed_ms: u128,
}

pub async fn localize_single_account(req: &LocalizeAccountRequest) -> anyhow::Result<String> {
    let mut src = redis_service::create_connection(&req.source).await?;
    let mut dst = redis_service::create_connection(&req.target).await?;

    let source_field = &req.source_field;
    let target_field = req
        .target_field
        .clone()
        .unwrap_or_else(|| format!("{}{}", req.server.pre_login, source_field));

    let raw = redis_service::get_hash_field_bytes_with_conn(&mut src, &req.hash_name, source_field)
        .await
        .with_context(|| {
            format!(
                "failed to read source hash field: hash={}, field={}",
                req.hash_name, source_field
            )
        })?;

    let encoded = localize_raw_msgpack(
        &raw,
        &req.server.platform,
        &req.server.group,
        &req.server.server,
    )?;

    redis_service::set_hash_field_bytes_with_conn(&mut dst, &req.hash_name, &target_field, encoded)
        .await
        .with_context(|| {
            format!(
                "failed to write target hash field: hash={}, field={}",
                req.hash_name, target_field
            )
        })?;

    Ok(target_field)
}

pub async fn localize_batch(req: &BatchLocalizeRequest) -> anyhow::Result<LocalizeSummary> {
    let started = Instant::now();
    let mut src = redis_service::create_connection(&req.source).await?;
    let mut dst = redis_service::create_connection(&req.target).await?;

    localize_entries_from_fields(
        &mut src,
        &mut dst,
        &req.hash_name,
        &req.source_fields,
        &req.server.platform,
        &req.server.group,
        &req.server.server,
        &req.server.pre_login,
        started,
    )
    .await
}

pub async fn localize_all_acc(req: &BatchLocalizeRequest) -> anyhow::Result<LocalizeSummary> {
    let started = Instant::now();

    let mut src = redis_service::create_connection(&req.source).await?;
    let mut dst = redis_service::create_connection(&req.target).await?;

    let mut cursor: u64 = 0;
    let mut scanned = 0usize;
    let mut localized = 0usize;
    let mut skipped = 0usize;
    let mut written = 0usize;
    let mut batch_items: Vec<(String, Vec<u8>)> = Vec::with_capacity(PIPELINE_BATCH_SIZE);

    loop {
        let (next_cursor, entries) =
            redis_service::scan_hash_entries(&mut src, &req.hash_name, cursor, HSCAN_COUNT)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("failed to scan entries from hash {}: {}", req.hash_name, e)
                })?;

        if entries.is_empty() && next_cursor == 0 {
            break;
        }

        for (field, raw) in entries {
            scanned += 1;

            let encoded = match localize_raw_msgpack(
                &raw,
                &req.server.platform,
                &req.server.group,
                &req.server.server,
            ) {
                Ok(v) => v,
                Err(err) => {
                    skipped += 1;
                    eprintln!(
                        "[WARN] localize field {} in hash {} failed: {}",
                        field, req.hash_name, err
                    );
                    continue;
                }
            };

            let target_field = format!("{}{}", req.server.pre_login, field);
            batch_items.push((target_field, encoded));
            localized += 1;

            if batch_items.len() >= PIPELINE_BATCH_SIZE {
                let n = batch_items.len();
                redis_service::set_hash_fields_bytes_pipeline(&mut dst, &req.hash_name, &batch_items)
                    .await
                    .with_context(|| {
                        format!("failed to write pipeline batch into hash {}", req.hash_name)
                    })?;
                written += n;
                batch_items.clear();
            }

            if scanned % PROGRESS_LOG_EVERY == 0 {
                println!(
                    "[localize_all_acc] hash={} scanned={} localized={} skipped={} written={} elapsed_ms={}",
                    req.hash_name,
                    scanned,
                    localized,
                    skipped,
                    written,
                    started.elapsed().as_millis()
                );
            }
        }

        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    if !batch_items.is_empty() {
        let n = batch_items.len();
        redis_service::set_hash_fields_bytes_pipeline(&mut dst, &req.hash_name, &batch_items)
            .await
            .with_context(|| format!("failed to write final pipeline batch into hash {}", req.hash_name))?;
        written += n;
    }

    Ok(LocalizeSummary {
        hash_name: req.hash_name.clone(),
        scanned,
        localized,
        skipped,
        written,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

async fn localize_entries_from_fields(
    src: &mut MultiplexedConnection,
    dst: &mut MultiplexedConnection,
    hash_name: &str,
    fields: &[String],
    platform: &str,
    group: &str,
    server: &str,
    pre_login: &str,
    started: Instant,
) -> anyhow::Result<LocalizeSummary> {
    let mut scanned = 0usize;
    let mut localized = 0usize;
    let mut skipped = 0usize;
    let mut written = 0usize;
    let mut batch_items: Vec<(String, Vec<u8>)> = Vec::with_capacity(PIPELINE_BATCH_SIZE);

    for field in fields {
        scanned += 1;

        let raw = match redis_service::get_hash_field_bytes_with_conn(src, hash_name, field).await {
            Ok(v) => v,
            Err(err) => {
                skipped += 1;
                eprintln!("[WARN] skip field {} in hash {}: {}", field, hash_name, err);
                continue;
            }
        };

        let encoded = match localize_raw_msgpack(&raw, platform, group, server) {
            Ok(v) => v,
            Err(err) => {
                skipped += 1;
                eprintln!(
                    "[WARN] localize field {} in hash {} failed: {}",
                    field, hash_name, err
                );
                continue;
            }
        };

        let target_field = format!("{}{}", pre_login, field);
        batch_items.push((target_field, encoded));
        localized += 1;

        if batch_items.len() >= PIPELINE_BATCH_SIZE {
            let n = batch_items.len();
            redis_service::set_hash_fields_bytes_pipeline(dst, hash_name, &batch_items)
                .await
                .with_context(|| format!("failed to write pipeline batch into hash {}", hash_name))?;
            written += n;
            batch_items.clear();
        }

        if scanned % PROGRESS_LOG_EVERY == 0 {
            println!(
                "[localize_batch] hash={} scanned={} localized={} skipped={} written={} elapsed_ms={}",
                hash_name,
                scanned,
                localized,
                skipped,
                written,
                started.elapsed().as_millis()
            );
        }
    }

    if !batch_items.is_empty() {
        let n = batch_items.len();
        redis_service::set_hash_fields_bytes_pipeline(dst, hash_name, &batch_items)
            .await
            .with_context(|| format!("failed to write final pipeline batch into hash {}", hash_name))?;
        written += n;
    }

    Ok(LocalizeSummary {
        hash_name: hash_name.to_string(),
        scanned,
        localized,
        skipped,
        written,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

fn localize_raw_msgpack(
    raw: &[u8],
    platform: &str,
    group: &str,
    server: &str,
) -> anyhow::Result<Vec<u8>> {
    let mut value = msgpack::decode_to_json_value(raw)?;
    transform_value(&mut value, platform, group, server)?;
    msgpack::encode_from_json_value(&value)
}

fn transform_value(
    value: &mut Value,
    platform: &str,
    group: &str,
    server: &str,
) -> anyhow::Result<()> {
    let platform_num = platform.parse::<i64>().ok();
    let group_num = group.parse::<i64>().ok();

    let server_re = Regex::new(r"\bS\d+\b")?;

    transform_node(
        value,
        platform,
        group,
        server,
        platform_num,
        group_num,
        &server_re,
        true,
    )
}

fn transform_node(
    value: &mut Value,
    platform: &str,
    group: &str,
    server: &str,
    platform_num: Option<i64>,
    group_num: Option<i64>,
    server_re: &Regex,
    is_root: bool,
) -> anyhow::Result<()> {
    match value {
        Value::String(s) => {
            let mut out = s.clone();
            out = replace_platform_like(&out, platform);
            out = replace_group_like(&out, group);
            out = replace_server_like(&out, server, server_re);
            *s = out;
        }
        Value::Array(arr) => {
            if is_root {
                patch_root_array(arr, platform_num, group_num);
            }

            for item in arr.iter_mut() {
                transform_node(
                    item,
                    platform,
                    group,
                    server,
                    platform_num,
                    group_num,
                    server_re,
                    false,
                )?;
            }
        }
        Value::Object(map) => {
            patch_object_keys(map, platform, group, server, platform_num, group_num);

            for (_, v) in map.iter_mut() {
                transform_node(
                    v,
                    platform,
                    group,
                    server,
                    platform_num,
                    group_num,
                    server_re,
                    false,
                )?;
            }
        }
        _ => {}
    }

    Ok(())
}

fn patch_root_array(arr: &mut [Value], platform_num: Option<i64>, group_num: Option<i64>) {
    if patch_array_platform_group(arr, platform_num, group_num) {
        return;
    }

    if let Some(Value::Array(inner)) = arr.get_mut(0) {
        let _ = patch_array_platform_group(inner, platform_num, group_num);
    }
}

fn patch_array_platform_group(
    arr: &mut [Value],
    platform_num: Option<i64>,
    group_num: Option<i64>,
) -> bool {
    if arr.len() < 2 {
        return false;
    }

    match (platform_num, group_num) {
        (Some(p), Some(g)) if is_numeric_like(&arr[0]) && is_numeric_like(&arr[1]) => {
            arr[0] = Value::Number(Number::from(p));
            arr[1] = Value::Number(Number::from(g));
            true
        }
        _ => false,
    }
}

fn patch_object_keys(
    map: &mut Map<String, Value>,
    platform: &str,
    group: &str,
    server: &str,
    platform_num: Option<i64>,
    group_num: Option<i64>,
) {
    for (k, v) in map.iter_mut() {
        match k.as_str() {
            "platform" | "plat" | "platformId" => patch_scalar_value(v, platform, platform_num),
            "group" | "groupId" | "gid" => patch_scalar_value(v, group, group_num),
            "server" | "sid" | "zone" => patch_string_value(v, server),
            _ => {}
        }
    }
}

fn patch_scalar_value(v: &mut Value, text_value: &str, num_value: Option<i64>) {
    match v {
        Value::Number(_) => {
            if let Some(n) = num_value {
                *v = Value::Number(Number::from(n));
            } else {
                *v = Value::String(text_value.to_string());
            }
        }
        Value::String(_) => {
            *v = Value::String(text_value.to_string());
        }
        _ => {}
    }
}

fn patch_string_value(v: &mut Value, text_value: &str) {
    if matches!(v, Value::String(_) | Value::Number(_)) {
        *v = Value::String(text_value.to_string());
    }
}

fn is_numeric_like(v: &Value) -> bool {
    match v {
        Value::Number(_) => true,
        Value::String(s) => s.parse::<i64>().is_ok(),
        _ => false,
    }
}

fn replace_platform_like(input: &str, platform: &str) -> String {
    let patterns = [
        r#""platform":"[^"]+""#,
        r#""plat":"[^"]+""#,
        r#""platformId":"[^"]+""#,
        r#"platform=[^,}\]]+"#,
        r#"plat=[^,}\]]+"#,
        r#"platformId=[^,}\]]+"#,
    ];

    let mut output = input.to_string();
    for p in patterns {
        let re = Regex::new(p).expect("invalid platform regex");
        output = re
            .replace_all(&output, |caps: &regex::Captures| {
                let text = caps.get(0).map(|m| m.as_str()).unwrap_or_default();
                if text.starts_with("\"platform\"") {
                    format!("\"platform\":\"{}\"", platform)
                } else if text.starts_with("\"plat\"") {
                    format!("\"plat\":\"{}\"", platform)
                } else if text.starts_with("\"platformId\"") {
                    format!("\"platformId\":\"{}\"", platform)
                } else if text.starts_with("platform=") {
                    format!("platform={}", platform)
                } else if text.starts_with("plat=") {
                    format!("plat={}", platform)
                } else if text.starts_with("platformId=") {
                    format!("platformId={}", platform)
                } else {
                    text.to_string()
                }
            })
            .to_string();
    }

    output
}

fn replace_group_like(input: &str, group: &str) -> String {
    let patterns = [
        r#""group":"[^"]+""#,
        r#""groupId":"[^"]+""#,
        r#""gid":"[^"]+""#,
        r#"group=[^,}\]]+"#,
        r#"groupId=[^,}\]]+"#,
        r#"gid=[^,}\]]+"#,
    ];

    let mut output = input.to_string();
    for p in patterns {
        let re = Regex::new(p).expect("invalid group regex");
        output = re
            .replace_all(&output, |caps: &regex::Captures| {
                let text = caps.get(0).map(|m| m.as_str()).unwrap_or_default();
                if text.starts_with("\"group\"") {
                    format!("\"group\":\"{}\"", group)
                } else if text.starts_with("\"groupId\"") {
                    format!("\"groupId\":\"{}\"", group)
                } else if text.starts_with("\"gid\"") {
                    format!("\"gid\":\"{}\"", group)
                } else if text.starts_with("group=") {
                    format!("group={}", group)
                } else if text.starts_with("groupId=") {
                    format!("groupId={}", group)
                } else if text.starts_with("gid=") {
                    format!("gid={}", group)
                } else {
                    text.to_string()
                }
            })
            .to_string();
    }

    output
}

fn replace_server_like(input: &str, server: &str, server_re: &Regex) -> String {
    let mut output = input.to_string();

    let kv_patterns = [
        r#""server":"[^"]+""#,
        r#""sid":"[^"]+""#,
        r#""zone":"[^"]+""#,
        r#"server=[^,}\]]+"#,
        r#"sid=[^,}\]]+"#,
        r#"zone=[^,}\]]+"#,
    ];

    for p in kv_patterns {
        let re = Regex::new(p).expect("invalid server kv regex");
        output = re
            .replace_all(&output, |caps: &regex::Captures| {
                let text = caps.get(0).map(|m| m.as_str()).unwrap_or_default();
                if text.starts_with("\"server\"") {
                    format!("\"server\":\"{}\"", server)
                } else if text.starts_with("\"sid\"") {
                    format!("\"sid\":\"{}\"", server)
                } else if text.starts_with("\"zone\"") {
                    format!("\"zone\":\"{}\"", server)
                } else if text.starts_with("server=") {
                    format!("server={}", server)
                } else if text.starts_with("sid=") {
                    format!("sid={}", server)
                } else if text.starts_with("zone=") {
                    format!("zone={}", server)
                } else {
                    text.to_string()
                }
            })
            .to_string();
    }

    server_re.replace_all(&output, server).to_string()
}
