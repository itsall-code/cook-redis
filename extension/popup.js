const DEFAULT_TIMEOUT = 60_000;
const LONG_TIMEOUT = 300_000;

const $ = (id) => document.getElementById(id);

const els = {
  logBox: $("log"),
  backendStatus: $("backendStatus"),

  envName: $("envName"),
  testTarget: $("testTarget"),

  hashName: $("hashName"),
  sourceField: $("sourceField"),
  targetField: $("targetField"),
  preLogin: $("preLogin"),
  server: $("server"),
  platform: $("platform"),
  group: $("group"),

  batchHashName: $("batchHashName"),
  batchPreLogin: $("batchPreLogin"),
  batchServer: $("batchServer"),
  batchPlatform: $("batchPlatform"),
  batchGroup: $("batchGroup"),

  deleteKeys: $("deleteKeys"),
  deleteTables: $("deleteTables"),

  viewRedisTarget: $("viewRedisTarget"),
  viewHashName: $("viewHashName"),
  viewField: $("viewField"),
  fieldList: $("fieldList"),
  fieldViewer: $("fieldViewer"),
  viewerSectionBody: $("viewerSectionBody"),
  toggleViewerSectionBtn: $("toggleViewerSectionBtn"),

  clearLogBtn: $("clearLogBtn"),
  testRedisBtn: $("testRedisBtn"),
  backupBtn: $("backupBtn"),
  localizeBtn: $("localizeBtn"),
  batchLocalizeBtn: $("batchLocalizeBtn"),
  deleteKeysBtn: $("deleteKeysBtn"),
  deleteTablesBtn: $("deleteTablesBtn"),
  listFieldsBtn: $("listFieldsBtn"),
  viewFieldBtn: $("viewFieldBtn"),
  flushBtn: $("flushBtn"),
};

function nowTime() {
  return new Date().toLocaleTimeString();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function appendLog(message, level = "info") {
  if (!els.logBox) return;

  const row = document.createElement("div");
  row.className = `log-item log-${level}`;

  const safeMessage =
    typeof message === "string" ? message : JSON.stringify(message, null, 2);

  row.textContent = `[${nowTime()}] ${safeMessage}`;
  els.logBox.prepend(row);
}

function clearLog() {
  if (els.logBox) els.logBox.innerHTML = "";
}

function setBackendStatus(text, ok = false, isError = false) {
  if (!els.backendStatus) return;
  els.backendStatus.textContent = `后端状态：${text}`;
  els.backendStatus.classList.remove("ok", "error", "warn");

  if (isError) {
    els.backendStatus.classList.add("error");
  } else if (ok) {
    els.backendStatus.classList.add("ok");
  } else {
    els.backendStatus.classList.add("warn");
  }
}

function normalizeLines(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function setButtonLoading(button, loading, loadingText = "处理中...") {
  if (!button) return;

  if (loading) {
    if (!button.dataset.originText) {
      button.dataset.originText = button.textContent;
    }
    button.textContent = loadingText;
    button.disabled = true;
  } else {
    button.textContent = button.dataset.originText || button.textContent;
    button.disabled = false;
  }
}

async function withButtonLoading(button, fn, loadingText = "处理中...") {
  try {
    setButtonLoading(button, true, loadingText);
    return await fn();
  } finally {
    setButtonLoading(button, false);
  }
}

async function apiFetch(url, data = {}, timeout = DEFAULT_TIMEOUT) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
      signal: controller.signal,
    });

    const text = await response.text();
    let parsed = null;

    try {
      parsed = text ? JSON.parse(text) : {};
    } catch {
      parsed = { raw: text };
    }

    if (!response.ok) {
      const message =
        parsed?.message ||
        parsed?.error ||
        parsed?.raw ||
        `HTTP ${response.status}`;
      return { ok: false, error: message, status: response.status, data: parsed };
    }

    return { ok: true, data: parsed };
  } catch (error) {
    if (error.name === "AbortError") {
      return { ok: false, error: "请求超时，请稍后重试" };
    }
    return { ok: false, error: error.message || "未知请求错误" };
  } finally {
    clearTimeout(timer);
  }
}

function buildRedisConfig(prefix) {
  return {
    host: localStorage.getItem(`${prefix}.host`) || "",
    port: Number(localStorage.getItem(`${prefix}.port`) || 6379),
    db: Number(localStorage.getItem(`${prefix}.db`) || 0),
    password: localStorage.getItem(`${prefix}.password`) || "",
  };
}

function buildServerConfig(prefix = "") {
  const p = prefix ? `${prefix}` : "";
  return {
    pre_login: (localStorage.getItem(`${p}pre_login`) || "").trim(),
    platform: (localStorage.getItem(`${p}platform`) || "").trim(),
    group: (localStorage.getItem(`${p}group`) || "").trim(),
    server: (localStorage.getItem(`${p}server`) || "").trim(),
  };
}

function getSingleServerConfig() {
  return {
    pre_login: (els.preLogin?.value || "").trim(),
    platform: (els.platform?.value || "").trim(),
    group: (els.group?.value || "").trim(),
    server: (els.server?.value || "").trim(),
  };
}

function getBatchServerConfig() {
  return {
    pre_login: (els.batchPreLogin?.value || "").trim(),
    platform: (els.batchPlatform?.value || "").trim(),
    group: (els.batchGroup?.value || "").trim(),
    server: (els.batchServer?.value || "").trim(),
  };
}

function currentEnvName() {
  return (els.envName?.value || localStorage.getItem("activeEnvName") || "default").trim();
}

function sourceRedisConfig() {
  const env = currentEnvName();
  return buildRedisConfig(`${env}.source`);
}

function targetRedisConfig() {
  const env = currentEnvName();
  return buildRedisConfig(`${env}.target`);
}

function validateRedisConfig(cfg, label) {
  if (!cfg.host) {
    throw new Error(`${label} host 不能为空`);
  }
  if (!cfg.port) {
    throw new Error(`${label} port 不能为空`);
  }
}

function validateServerConfig(serverCfg) {
  if (!serverCfg.server) throw new Error("server 不能为空");
  if (!serverCfg.platform) throw new Error("platform 不能为空");
  if (!serverCfg.group) throw new Error("group 不能为空");
}

function buildSingleLocalizePayload() {
  const hash_name = (els.hashName?.value || "").trim();
  const source_field = (els.sourceField?.value || "").trim();
  const target_field = (els.targetField?.value || "").trim();
  const server = getSingleServerConfig();

  if (!hash_name) throw new Error("Hash 名不能为空");
  if (!source_field) throw new Error("源账号 Field 不能为空");
  validateServerConfig(server);

  const source = sourceRedisConfig();
  const target = targetRedisConfig();

  validateRedisConfig(source, "source Redis");
  validateRedisConfig(target, "target Redis");

  return {
    source,
    target,
    hash_name,
    source_field,
    target_field: target_field || null,
    server,
  };
}

function buildBatchLocalizePayload() {
  const hash_name = (els.batchHashName?.value || "").trim();
  const server = getBatchServerConfig();

  if (!hash_name) throw new Error("Hash 名不能为空");
  validateServerConfig(server);

  const source = sourceRedisConfig();
  const target = targetRedisConfig();

  validateRedisConfig(source, "source Redis");
  validateRedisConfig(target, "target Redis");

  return {
    source,
    target,
    hash_name,
    source_fields: [],
    server,
  };
}

function buildTestRedisPayload() {
  const targetType = els.testTarget?.value || "source";
  const config = targetType === "target" ? targetRedisConfig() : sourceRedisConfig();
  validateRedisConfig(config, `${targetType} Redis`);

  return {
    target: targetType,
    config,
  };
}

function buildBackupPayload() {
  const source = sourceRedisConfig();
  const target = targetRedisConfig();
  validateRedisConfig(source, "source Redis");
  validateRedisConfig(target, "target Redis");

  return { source, target };
}

function buildDeleteKeysPayload() {
  const target = targetRedisConfig();
  validateRedisConfig(target, "target Redis");

  return {
    target,
    keys: normalizeLines(els.deleteKeys?.value || ""),
  };
}

function buildDeleteTablesPayload() {
  const target = targetRedisConfig();
  validateRedisConfig(target, "target Redis");

  return {
    target,
    tables: normalizeLines(els.deleteTables?.value || ""),
  };
}

function buildListFieldsPayload() {
  const hash_name = (els.viewHashName?.value || "").trim();
  const targetName = els.viewRedisTarget?.value || "source";
  const config = targetName === "target" ? targetRedisConfig() : sourceRedisConfig();
  validateRedisConfig(config, `${targetName} Redis`);

  if (!hash_name) throw new Error("Hash 名不能为空");

  return {
    target: targetName,
    config,
    hash_name,
  };
}

function buildViewFieldPayload() {
  const hash_name = (els.viewHashName?.value || "").trim();
  const field = (els.viewField?.value || "").trim();
  const targetName = els.viewRedisTarget?.value || "source";
  const config = targetName === "target" ? targetRedisConfig() : sourceRedisConfig();
  validateRedisConfig(config, `${targetName} Redis`);

  if (!hash_name) throw new Error("Hash 名不能为空");
  if (!field) throw new Error("Field 不能为空");

  return {
    target: targetName,
    config,
    hash_name,
    field,
  };
}

function renderFieldList(fields = []) {
  if (!els.fieldList) return;

  if (!Array.isArray(fields) || fields.length === 0) {
    els.fieldList.style.display = "block";
    els.fieldList.innerHTML = `<div class="small">没有读取到字段</div>`;
    return;
  }

  const items = fields
    .map(
      (field) =>
        `<button type="button" class="field-chip" data-field="${escapeHtml(field)}">${escapeHtml(field)}</button>`
    )
    .join("");

  els.fieldList.style.display = "block";
  els.fieldList.innerHTML = `
    <div class="viewer-title">字段列表（${fields.length}）</div>
    <div class="field-chip-list">${items}</div>
  `;

  els.fieldList.querySelectorAll("[data-field]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const field = btn.getAttribute("data-field") || "";
      if (els.viewField) els.viewField.value = field;
      appendLog(`已选择字段：${field}`, "info");
    });
  });
}

function renderViewer(data) {
  if (!els.fieldViewer) return;

  const pretty = typeof data === "string" ? data : JSON.stringify(data, null, 2);

  els.fieldViewer.innerHTML = `
    <div class="viewer-title">读取结果</div>
    <pre class="viewer-pre">${escapeHtml(pretty)}</pre>
  `;
}

function toggleViewerSection() {
  if (!els.viewerSectionBody || !els.toggleViewerSectionBtn) return;

  const collapsed = els.viewerSectionBody.classList.contains("collapsed");
  els.viewerSectionBody.classList.toggle("collapsed", !collapsed);
  els.toggleViewerSectionBtn.textContent = collapsed ? "收起" : "展开";
}

async function refreshBackendStatus() {
  setBackendStatus("检查中...", false, false);

  const source = sourceRedisConfig();
  if (!source.host) {
    setBackendStatus("未配置 source Redis", false, true);
    return;
  }

  const result = await apiFetch("/api/redis/test", {
    target: "source",
    config: source,
  }, 15_000);

  if (result.ok) {
    setBackendStatus("可用", true, false);
  } else {
    setBackendStatus(result.error || "不可用", false, true);
  }
}

async function refreshEnvSelect() {
  if (!els.envName) return;

  const savedList = localStorage.getItem("envNames");
  let envNames = [];

  try {
    envNames = savedList ? JSON.parse(savedList) : [];
  } catch {
    envNames = [];
  }

  if (!Array.isArray(envNames) || envNames.length === 0) {
    envNames = ["default"];
  }

  els.envName.innerHTML = envNames
    .map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`)
    .join("");

  const active = localStorage.getItem("activeEnvName") || envNames[0] || "default";
  els.envName.value = envNames.includes(active) ? active : envNames[0];
  localStorage.setItem("activeEnvName", els.envName.value);
}

async function ensureDefaultState() {
  if (!localStorage.getItem("envNames")) {
    localStorage.setItem("envNames", JSON.stringify(["default"]));
  }
  if (!localStorage.getItem("activeEnvName")) {
    localStorage.setItem("activeEnvName", "default");
  }
}

async function fillDefaults() {
  const single = buildServerConfig("");
  const batch = buildServerConfig("batch.");

  if (els.preLogin) els.preLogin.value = single.pre_login || "";
  if (els.server) els.server.value = single.server || "";
  if (els.platform) els.platform.value = single.platform || "";
  if (els.group) els.group.value = single.group || "";

  if (els.batchPreLogin) els.batchPreLogin.value = batch.pre_login || single.pre_login || "";
  if (els.batchServer) els.batchServer.value = batch.server || single.server || "";
  if (els.batchPlatform) els.batchPlatform.value = batch.platform || single.platform || "";
  if (els.batchGroup) els.batchGroup.value = batch.group || single.group || "";

  if (els.hashName && !els.hashName.value) els.hashName.value = "Account";
  if (els.batchHashName && !els.batchHashName.value) els.batchHashName.value = "Account";
  if (els.viewHashName && !els.viewHashName.value) els.viewHashName.value = "Account";
}

function persistCurrentEnv() {
  if (els.envName?.value) {
    localStorage.setItem("activeEnvName", els.envName.value);
  }
}

async function switchEnv() {
  persistCurrentEnv();
  appendLog(`已切换环境：${currentEnvName()}`, "info");
  await fillDefaults();
  await refreshBackendStatus();
}

async function testRedisConnection() {
  await withButtonLoading(els.testRedisBtn, async () => {
    try {
      const payload = buildTestRedisPayload();
      appendLog(`开始测试 ${payload.target} Redis 连接...`, "info");

      const result = await apiFetch("/api/redis/test", payload, 20_000);

      if (!result.ok) {
        appendLog(`Redis 连接失败：${result.error}`, "error");
        return;
      }

      appendLog(`${payload.target} Redis 连接成功`, "ok");
      setBackendStatus("可用", true, false);
    } catch (error) {
      appendLog(`Redis 连接失败：${error.message}`, "error");
    }
  }, "测试中...");
}

async function backupDb() {
  await withButtonLoading(els.backupBtn, async () => {
    try {
      const payload = buildBackupPayload();

      if (!confirm("确认执行备份？此操作会覆盖目标库。")) return;

      appendLog("开始备份数据库...", "info");
      const result = await apiFetch("/api/redis/backup", payload, LONG_TIMEOUT);

      if (!result.ok) {
        appendLog(`备份失败：${result.error}`, "error");
        return;
      }

      const copied =
        result.data?.data?.copied ??
        result.data?.copied ??
        result.data?.data ??
        "未知";

      appendLog(`备份完成，复制数量：${copied}`, "ok");
    } catch (error) {
      appendLog(`备份失败：${error.message}`, "error");
    }
  }, "备份中...");
}

async function localizeAccount() {
  await withButtonLoading(els.localizeBtn, async () => {
    try {
      const payload = buildSingleLocalizePayload();
      appendLog(`开始单账号本地化：${payload.hash_name} / ${payload.source_field}`, "info");

      const result = await apiFetch("/api/process/localize-account", payload, LONG_TIMEOUT);

      if (!result.ok) {
        appendLog(`单账号本地化失败：${result.error}`, "error");
        return;
      }

      const targetField =
        result.data?.data ||
        result.data?.target_field ||
        payload.target_field ||
        "已完成";

      appendLog(`单账号本地化成功，目标字段：${targetField}`, "ok");
    } catch (error) {
      appendLog(`单账号本地化失败：${error.message}`, "error");
    }
  }, "本地化中...");
}

async function batchLocalize() {
  await withButtonLoading(els.batchLocalizeBtn, async () => {
    try {
      const payload = buildBatchLocalizePayload();

      if (!confirm("确认执行全表本地化？")) return;

      appendLog(`开始全表本地化：${payload.hash_name}`, "info");

      const result = await apiFetch("/api/process/localize-all-acc", payload, LONG_TIMEOUT);

      if (!result.ok) {
        appendLog(`全表本地化失败：${result.error}`, "error");
        return;
      }

      const summary = result.data?.data || result.data || {};
      const scanned = summary.scanned ?? 0;
      const localized = summary.localized ?? 0;
      const skipped = summary.skipped ?? 0;
      const written = summary.written ?? localized;
      const elapsed = summary.elapsed_ms ?? 0;

      appendLog(
        `全表本地化完成：scanned=${scanned}, localized=${localized}, skipped=${skipped}, written=${written}, elapsed=${elapsed}ms`,
        "ok"
      );
    } catch (error) {
      appendLog(`全表本地化失败：${error.message}`, "error");
    }
  }, "执行中...");
}

async function deleteKeys() {
  await withButtonLoading(els.deleteKeysBtn, async () => {
    try {
      const payload = buildDeleteKeysPayload();

      if (!payload.keys.length) {
        throw new Error("请至少输入一个 key");
      }

      if (!confirm(`确认删除 ${payload.keys.length} 个 key？`)) return;

      appendLog(`开始删除 ${payload.keys.length} 个 keys...`, "info");
      const result = await apiFetch("/api/redis/delete-keys", payload, LONG_TIMEOUT);

      if (!result.ok) {
        appendLog(`删除 Keys 失败：${result.error}`, "error");
        return;
      }

      const deleted =
        result.data?.data?.deleted ??
        result.data?.deleted ??
        result.data?.data ??
        payload.keys.length;

      appendLog(`删除 Keys 完成，删除数量：${deleted}`, "ok");
    } catch (error) {
      appendLog(`删除 Keys 失败：${error.message}`, "error");
    }
  }, "删除中...");
}

async function deleteTables() {
  await withButtonLoading(els.deleteTablesBtn, async () => {
    try {
      const payload = buildDeleteTablesPayload();

      if (!payload.tables.length) {
        throw new Error("请至少输入一个 table");
      }

      if (!confirm(`确认删除 ${payload.tables.length} 个 table？`)) return;

      appendLog(`开始删除 ${payload.tables.length} 个 tables...`, "info");
      const result = await apiFetch("/api/redis/delete-tables", payload, LONG_TIMEOUT);

      if (!result.ok) {
        appendLog(`删除 Tables 失败：${result.error}`, "error");
        return;
      }

      const deleted =
        result.data?.data?.deleted ??
        result.data?.deleted ??
        result.data?.data ??
        payload.tables.length;

      appendLog(`删除 Tables 完成，删除数量：${deleted}`, "ok");
    } catch (error) {
      appendLog(`删除 Tables 失败：${error.message}`, "error");
    }
  }, "删除中...");
}

async function listFields() {
  await withButtonLoading(els.listFieldsBtn, async () => {
    try {
      const payload = buildListFieldsPayload();
      appendLog(`开始读取字段列表：${payload.hash_name}`, "info");

      const result = await apiFetch("/api/redis/hash/fields", payload, LONG_TIMEOUT);

      if (!result.ok) {
        appendLog(`列出字段失败：${result.error}`, "error");
        return;
      }

      const fields =
        result.data?.data?.fields ||
        result.data?.fields ||
        result.data?.data ||
        [];

      renderFieldList(fields);
      appendLog(`字段读取完成，共 ${Array.isArray(fields) ? fields.length : 0} 个`, "ok");
    } catch (error) {
      appendLog(`列出字段失败：${error.message}`, "error");
    }
  }, "读取中...");
}

async function viewField() {
  await withButtonLoading(els.viewFieldBtn, async () => {
    try {
      const payload = buildViewFieldPayload();
      appendLog(`开始读取字段：${payload.hash_name} / ${payload.field}`, "info");

      const result = await apiFetch("/api/redis/hash/visualize", payload, LONG_TIMEOUT);

      if (!result.ok) {
        appendLog(`字段可视化失败：${result.error}`, "error");
        return;
      }

      const data = result.data?.data || result.data || {};
      renderViewer(data);
      appendLog(`字段读取成功：${payload.field}`, "ok");
    } catch (error) {
      appendLog(`字段可视化失败：${error.message}`, "error");
    }
  }, "读取中...");
}

async function flushDb() {
  await withButtonLoading(els.flushBtn, async () => {
    try {
      const target = targetRedisConfig();
      validateRedisConfig(target, "target Redis");

      const text = prompt('危险操作：请输入 "FLUSHDB" 以确认清空目标 DB');
      if (text !== "FLUSHDB") {
        appendLog("已取消清空 DB", "warn");
        return;
      }

      appendLog("开始清空目标 DB...", "warn");
      const result = await apiFetch("/api/redis/flushdb", { target }, LONG_TIMEOUT);

      if (!result.ok) {
        appendLog(`清空 DB 失败：${result.error}`, "error");
        return;
      }

      appendLog("目标 DB 已清空", "ok");
    } catch (error) {
      appendLog(`清空 DB 失败：${error.message}`, "error");
    }
  }, "清空中...");
}

function bindEvents() {
  els.envName?.addEventListener("change", switchEnv);
  els.clearLogBtn?.addEventListener("click", clearLog);
  els.testRedisBtn?.addEventListener("click", testRedisConnection);
  els.backupBtn?.addEventListener("click", backupDb);
  els.localizeBtn?.addEventListener("click", localizeAccount);
  els.batchLocalizeBtn?.addEventListener("click", batchLocalize);
  els.deleteKeysBtn?.addEventListener("click", deleteKeys);
  els.deleteTablesBtn?.addEventListener("click", deleteTables);
  els.listFieldsBtn?.addEventListener("click", listFields);
  els.viewFieldBtn?.addEventListener("click", viewField);
  els.flushBtn?.addEventListener("click", flushDb);
  els.toggleViewerSectionBtn?.addEventListener("click", toggleViewerSection);
}

document.addEventListener("DOMContentLoaded", async () => {
  bindEvents();

  try {
    await ensureDefaultState();
    await refreshEnvSelect();
    await fillDefaults();
    await refreshBackendStatus();
    appendLog("插件初始化完成", "ok");
  } catch (error) {
    appendLog(`插件初始化失败：${error.message}`, "error");
    setBackendStatus(`初始化失败：${error.message}`, false, true);
  }
});
