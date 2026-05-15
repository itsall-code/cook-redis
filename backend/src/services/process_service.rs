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

const HSCAN_COUNT: usize = 2_000;
const PIPELINE_BATCH_SIZE: usize = 1_000;
const HMGET_BATCH_SIZE: usize = 1_000;
const PROGRESS_LOG_EVERY: usize = 2_000;

#[derive(Debug, Serialize, Clone)]
pub struct LocalizeSummary {
    pub hash_name: String,
    pub scanned: usize,
    pub localized: usize,
    pub skipped: usize,
    pub written: usize,
    pub elapsed_ms: u128,
}

struct LocalizeContext {
    platform: String,
    group: String,
    server: String,
    platform_num: Option<i64>,
    group_num: Option<i64>,
    server_re: Regex,
    platform_patterns: Vec<(Regex, PlatformReplacement)>,
    group_patterns: Vec<(Regex, GroupReplacement)>,
    server_patterns: Vec<(Regex, ServerReplacement)>,
}

#[derive(Clone, Copy)]
enum PlatformReplacement {
    PlatformJson,
    PlatJson,
    PlatformIdJson,
    PlatformKv,
    PlatKv,
    PlatformIdKv,
}

#[derive(Clone, Copy)]
enum GroupReplacement {
    GroupJson,
    GroupIdJson,
    GidJson,
    GroupKv,
    GroupIdKv,
    GidKv,
}

#[derive(Clone, Copy)]
enum ServerReplacement {
    ServerJson,
    SidJson,
    ZoneJson,
    ServerKv,
    SidKv,
    ZoneKv,
}

impl LocalizeContext {
    fn new(platform: &str, group: &str, server: &str) -> anyhow::Result<Self> {
        Ok(Self {
            platform: platform.to_string(),
            group: group.to_string(),
            server: server.to_string(),
            platform_num: platform.parse::<i64>().ok(),
            group_num: group.parse::<i64>().ok(),
            server_re: Regex::new(r"\bS\d+\b")?,
            platform_patterns: vec![
                (
                    Regex::new(r#""platform":"[^"]+""#)?,
                    PlatformReplacement::PlatformJson,
                ),
                (
                    Regex::new(r#""plat":"[^"]+""#)?,
                    PlatformReplacement::PlatJson,
                ),
                (
                    Regex::new(r#""platformId":"[^"]+""#)?,
                    PlatformReplacement::PlatformIdJson,
                ),
                (
                    Regex::new(r#"platform=[^,}\]]+"#)?,
                    PlatformReplacement::PlatformKv,
                ),
                (Regex::new(r#"plat=[^,}\]]+"#)?, PlatformReplacement::PlatKv),
                (
                    Regex::new(r#"platformId=[^,}\]]+"#)?,
                    PlatformReplacement::PlatformIdKv,
                ),
            ],
            group_patterns: vec![
                (
                    Regex::new(r#""group":"[^"]+""#)?,
                    GroupReplacement::GroupJson,
                ),
                (
                    Regex::new(r#""groupId":"[^"]+""#)?,
                    GroupReplacement::GroupIdJson,
                ),
                (Regex::new(r#""gid":"[^"]+""#)?, GroupReplacement::GidJson),
                (Regex::new(r#"group=[^,}\]]+"#)?, GroupReplacement::GroupKv),
                (
                    Regex::new(r#"groupId=[^,}\]]+"#)?,
                    GroupReplacement::GroupIdKv,
                ),
                (Regex::new(r#"gid=[^,}\]]+"#)?, GroupReplacement::GidKv),
            ],
            server_patterns: vec![
                (
                    Regex::new(r#""server":"[^"]+""#)?,
                    ServerReplacement::ServerJson,
                ),
                (Regex::new(r#""sid":"[^"]+""#)?, ServerReplacement::SidJson),
                (
                    Regex::new(r#""zone":"[^"]+""#)?,
                    ServerReplacement::ZoneJson,
                ),
                (
                    Regex::new(r#"server=[^,}\]]+"#)?,
                    ServerReplacement::ServerKv,
                ),
                (Regex::new(r#"sid=[^,}\]]+"#)?, ServerReplacement::SidKv),
                (Regex::new(r#"zone=[^,}\]]+"#)?, ServerReplacement::ZoneKv),
            ],
        })
    }
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

    let ctx = LocalizeContext::new(&req.server.platform, &req.server.group, &req.server.server)?;
    let encoded = localize_raw_msgpack(&raw, &ctx)?;

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

    let ctx = LocalizeContext::new(&req.server.platform, &req.server.group, &req.server.server)?;

    localize_entries_from_fields(
        &mut src,
        &mut dst,
        &req.hash_name,
        &req.source_fields,
        &ctx,
        &req.server.pre_login,
        started,
    )
    .await
}

pub async fn localize_all_acc(req: &BatchLocalizeRequest) -> anyhow::Result<LocalizeSummary> {
    let started = Instant::now();

    let mut src = redis_service::create_connection(&req.source).await?;
    let mut dst = redis_service::create_connection(&req.target).await?;
    let ctx = LocalizeContext::new(&req.server.platform, &req.server.group, &req.server.server)?;

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

            let encoded = match localize_raw_msgpack(&raw, &ctx) {
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
                redis_service::set_hash_fields_bytes_pipeline(
                    &mut dst,
                    &req.hash_name,
                    &batch_items,
                )
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
            .with_context(|| {
                format!(
                    "failed to write final pipeline batch into hash {}",
                    req.hash_name
                )
            })?;
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
    ctx: &LocalizeContext,
    pre_login: &str,
    started: Instant,
) -> anyhow::Result<LocalizeSummary> {
    let mut scanned = 0usize;
    let mut localized = 0usize;
    let mut skipped = 0usize;
    let mut written = 0usize;
    let mut batch_items: Vec<(String, Vec<u8>)> = Vec::with_capacity(PIPELINE_BATCH_SIZE);

    for fields_chunk in fields.chunks(HMGET_BATCH_SIZE) {
        let values =
            redis_service::get_hash_fields_bytes_with_conn(src, hash_name, fields_chunk).await?;

        for (field, raw) in fields_chunk.iter().zip(values) {
            scanned += 1;

            let Some(raw) = raw else {
                skipped += 1;
                eprintln!("[WARN] skip missing field {} in hash {}", field, hash_name);
                continue;
            };

            let encoded = match localize_raw_msgpack(&raw, ctx) {
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
                    .with_context(|| {
                        format!("failed to write pipeline batch into hash {}", hash_name)
                    })?;
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
    }

    if !batch_items.is_empty() {
        let n = batch_items.len();
        redis_service::set_hash_fields_bytes_pipeline(dst, hash_name, &batch_items)
            .await
            .with_context(|| {
                format!(
                    "failed to write final pipeline batch into hash {}",
                    hash_name
                )
            })?;
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

fn localize_raw_msgpack(raw: &[u8], ctx: &LocalizeContext) -> anyhow::Result<Vec<u8>> {
    let mut value = msgpack::decode_to_json_value(raw)?;
    transform_value(&mut value, ctx)?;
    msgpack::encode_from_json_value(&value)
}

fn transform_value(value: &mut Value, ctx: &LocalizeContext) -> anyhow::Result<()> {
    transform_node(value, ctx, true)
}

fn transform_node(value: &mut Value, ctx: &LocalizeContext, is_root: bool) -> anyhow::Result<()> {
    match value {
        Value::String(s) => {
            let mut out = s.clone();
            out = replace_platform_like(&out, ctx);
            out = replace_group_like(&out, ctx);
            out = replace_server_like(&out, ctx);
            *s = out;
        }
        Value::Array(arr) => {
            if is_root {
                patch_root_array(arr, ctx.platform_num, ctx.group_num);
            }

            for item in arr.iter_mut() {
                transform_node(item, ctx, false)?;
            }
        }
        Value::Object(map) => {
            patch_object_keys(map, ctx);

            for (_, v) in map.iter_mut() {
                transform_node(v, ctx, false)?;
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

fn patch_object_keys(map: &mut Map<String, Value>, ctx: &LocalizeContext) {
    for (k, v) in map.iter_mut() {
        match k.as_str() {
            "platform" | "plat" | "platformId" => {
                patch_scalar_value(v, &ctx.platform, ctx.platform_num)
            }
            "group" | "groupId" | "gid" => patch_scalar_value(v, &ctx.group, ctx.group_num),
            "server" | "sid" | "zone" => patch_string_value(v, &ctx.server),
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

fn replace_platform_like(input: &str, ctx: &LocalizeContext) -> String {
    let mut output = input.to_string();
    for (re, replacement) in &ctx.platform_patterns {
        let replacement = match replacement {
            PlatformReplacement::PlatformJson => format!(r#""platform":"{}""#, ctx.platform),
            PlatformReplacement::PlatJson => format!(r#""plat":"{}""#, ctx.platform),
            PlatformReplacement::PlatformIdJson => format!(r#""platformId":"{}""#, ctx.platform),
            PlatformReplacement::PlatformKv => format!("platform={}", ctx.platform),
            PlatformReplacement::PlatKv => format!("plat={}", ctx.platform),
            PlatformReplacement::PlatformIdKv => format!("platformId={}", ctx.platform),
        };
        output = re.replace_all(&output, replacement.as_str()).to_string();
    }

    output
}

fn replace_group_like(input: &str, ctx: &LocalizeContext) -> String {
    let mut output = input.to_string();
    for (re, replacement) in &ctx.group_patterns {
        let replacement = match replacement {
            GroupReplacement::GroupJson => format!(r#""group":"{}""#, ctx.group),
            GroupReplacement::GroupIdJson => format!(r#""groupId":"{}""#, ctx.group),
            GroupReplacement::GidJson => format!(r#""gid":"{}""#, ctx.group),
            GroupReplacement::GroupKv => format!("group={}", ctx.group),
            GroupReplacement::GroupIdKv => format!("groupId={}", ctx.group),
            GroupReplacement::GidKv => format!("gid={}", ctx.group),
        };
        output = re.replace_all(&output, replacement.as_str()).to_string();
    }

    output
}

fn replace_server_like(input: &str, ctx: &LocalizeContext) -> String {
    let mut output = input.to_string();
    for (re, replacement) in &ctx.server_patterns {
        let replacement = match replacement {
            ServerReplacement::ServerJson => format!(r#""server":"{}""#, ctx.server),
            ServerReplacement::SidJson => format!(r#""sid":"{}""#, ctx.server),
            ServerReplacement::ZoneJson => format!(r#""zone":"{}""#, ctx.server),
            ServerReplacement::ServerKv => format!("server={}", ctx.server),
            ServerReplacement::SidKv => format!("sid={}", ctx.server),
            ServerReplacement::ZoneKv => format!("zone={}", ctx.server),
        };
        output = re.replace_all(&output, replacement.as_str()).to_string();
    }

    ctx.server_re
        .replace_all(&output, ctx.server.as_str())
        .to_string()
}
