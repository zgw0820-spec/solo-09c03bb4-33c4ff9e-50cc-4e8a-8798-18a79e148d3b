"use strict";

(() => {
  const $ = (id) => document.getElementById(id);

  const els = {
    form: $("upload-form"),
    file: $("file-input"),
    sha: $("sha-input"),
    start: $("start-btn"),
    reset: $("reset-btn"),
    setupError: $("setup-error"),
    sessionCard: $("session-card"),
    sessionId: $("session-id"),
    sessionFile: $("session-file"),
    sessionSize: $("session-size"),
    sessionChunks: $("session-chunks"),
    sessionStatus: $("session-status"),
    progressBar: $("progress-bar"),
    progressText: $("progress-text"),
    resume: $("resume-btn"),
    complete: $("complete-btn"),
    download: $("download-btn"),
    cancel: $("cancel-btn"),
    sessionError: $("session-error"),
    sessionNote: $("session-note"),
    chunkList: $("chunk-list"),
    log: $("log"),
  };

  const state = {
    file: null,
    session: null,
    received: new Set(),
    running: false,
    stopped: false,
    controller: null,
  };

  const STORE_PREFIX = "artifact-upload:";
  const MAX_RETRIES = 4;
  const PARALLEL = 3;

  class HttpError extends Error {
    constructor(status, detail) {
      super((detail && detail.message) || `HTTP ${status}`);
      this.status = status;
      this.detail = detail || {};
    }
  }

  class UiError extends Error {}

  async function api(path, options = {}) {
    const response = await fetch(path, options);
    const type = response.headers.get("content-type") || "";
    let body = null;
    if (type.includes("application/json")) {
      body = await response.json().catch(() => null);
    }
    if (!response.ok) {
      throw new HttpError(response.status, body && body.detail);
    }
    return body;
  }

  function log(message, kind) {
    const item = document.createElement("li");
    item.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    if (kind === "error") item.style.color = "var(--bad)";
    els.log.prepend(item);
    while (els.log.children.length > 100) els.log.lastChild.remove();
  }

  function showError(el, message) {
    el.hidden = !message;
    el.textContent = message || "";
  }

  function showNote(message) {
    els.sessionNote.hidden = !message;
    els.sessionNote.textContent = message || "";
  }

  function formatBytes(n) {
    if (n === 0) return "0 B";
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    const exp = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
    return `${(n / 1024 ** exp).toFixed(exp === 0 ? 0 : 1)} ${units[exp]}`;
  }

  const ERROR_TEXT = {
    digest_mismatch: "合并后的整包 SHA-256 与填写值不一致，未登记对象。请确认摘要或重新选择正确文件。",
    chunk_digest_conflict: "该序号已有不同内容的分片，服务端已拒绝覆盖。请核对文件是否被改动。",
    chunk_digest_mismatch: "分片在传输中发生变化（摘要不符），请重试该分片。",
    chunk_size_mismatch: "分片大小与预期不符，请确认所选文件与会话一致。",
    chunks_missing: "仍有分片未确认，无法完成。",
    upload_completed: "该会话已完成，无需再传分片。",
    upload_not_found: "上传会话不存在，可能已被清理。",
  };

  function describeError(err) {
    if (err instanceof UiError) return err.message;
    if (err instanceof HttpError) {
      const d = err.detail;
      const friendly = ERROR_TEXT[d.code];
      let text = friendly || `${d.code || "error"}: ${d.message || err.message}`;
      if (Array.isArray(d.missing_chunks) && d.missing_chunks.length) {
        const shown = d.missing_chunks.slice(0, 12).join(", ");
        const more = d.missing_chunks.length > 12 ? ` …（共 ${d.missing_chunks.length} 片）` : "";
        text += `\n缺少分片: ${shown}${more}`;
      }
      return text;
    }
    if (err && err.name === "AbortError") return "已停止";
    return `网络错误: ${err && err.message ? err.message : err}（可重试）`;
  }

  function setBusy(busy) {
    state.running = busy;
    els.start.disabled = busy;
    els.resume.disabled = busy;
    els.complete.disabled = busy || (state.session && state.session.status === "completed");
    els.cancel.hidden = !busy;
    els.cancel.disabled = false;
  }

  function storeKey(session) {
    return `${STORE_PREFIX}${session.expected_sha256}:${session.file_size}:${session.filename}`;
  }

  function rememberSession(session) {
    try {
      localStorage.setItem(storeKey(session), session.session_id);
      localStorage.setItem(`${STORE_PREFIX}last`, session.session_id);
    } catch (_) { /* private mode etc. */ }
  }

  // ---------------------------------------------------------------- rendering

  function render() {
    const s = state.session;
    if (!s) {
      els.sessionCard.hidden = true;
      return;
    }
    els.sessionCard.hidden = false;
    els.sessionId.textContent = s.session_id;
    els.sessionFile.textContent = s.filename;
    els.sessionSize.textContent = `${formatBytes(s.file_size)}（${s.file_size} 字节）`;
    els.sessionChunks.textContent =
      `${s.total_chunks} 片 × ${formatBytes(s.chunk_size)}` +
      (s.file_size === 0 ? "（空文件）" : "");
    els.sessionStatus.textContent = s.status === "completed" ? "已完成" : "上传中";
    els.sessionStatus.className = `badge ${s.status}`;

    const total = s.total_chunks;
    const done = state.received.size;
    els.progressBar.style.width = total === 0 ? "100%" : `${Math.round((done / total) * 100)}%`;
    els.progressText.textContent = total === 0
      ? "空文件，无需传输分片"
      : `${done} / ${total} 片已确认`;

    const completed = s.status === "completed";
    els.download.hidden = !completed;
    els.complete.disabled = completed || state.running;
    els.resume.disabled = completed || state.running;
    if (completed && s.download_url) {
      els.download.onclick = () => {
        const a = document.createElement("a");
        a.href = s.download_url;
        a.download = s.filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
      };
    }
    renderChunks();
  }

  function renderChunks() {
    const s = state.session;
    if (!s) return;
    els.chunkList.textContent = "";
    const limit = Math.min(s.total_chunks, 512);
    const fragment = document.createDocumentFragment();
    for (let i = 0; i < limit; i += 1) {
      const box = document.createElement("span");
      box.className = state.received.has(i) ? "chunk done" : "chunk";
      box.textContent = String(i);
      box.title = `分片 ${i}`;
      fragment.appendChild(box);
    }
    els.chunkList.appendChild(fragment);
    if (s.total_chunks > limit) {
      const note = document.createElement("p");
      note.className = "hint";
      note.textContent = `仅显示前 ${limit} 片，共 ${s.total_chunks} 片。`;
      els.chunkList.appendChild(note);
    }
  }

  function applySession(session) {
    state.session = session;
    state.received = new Set((session.received_chunks || []).map((c) => c.idx));
    render();
  }

  // ------------------------------------------------------------------ crypto

  async function sha256Hex(buffer) {
    const digest = await crypto.subtle.digest("SHA-256", buffer);
    return Array.from(new Uint8Array(digest))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }

  async function readChunk(index, chunkSize) {
    const start = index * chunkSize;
    const end = Math.min(start + chunkSize, state.file.size);
    const buffer = await state.file.slice(start, end).arrayBuffer();
    return { buffer, digest: await sha256Hex(buffer), size: buffer.byteLength };
  }

  // ----------------------------------------------------------------- upload

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function signal() {
    return state.controller ? state.controller.signal : undefined;
  }

  async function refreshSession() {
    const fresh = await api(`/api/uploads/${state.session.session_id}`);
    applySession(fresh);
    return fresh;
  }

  async function uploadChunk(index) {
    const s = state.session;
    const { buffer, digest, size } = await readChunk(index, s.chunk_size);
    if (state.stopped) throw new DOMException("aborted", "AbortError");

    for (let attempt = 0; ; attempt += 1) {
      try {
        const form = new FormData();
        form.append("chunk", new Blob([buffer]), `chunk-${index}`);
        await api(`/api/uploads/${s.session_id}/chunks/${index}`, {
          method: "PUT",
          headers: { "X-Chunk-SHA256": digest },
          body: form,
          signal: signal(),
        });
        state.received.add(index);
        render();
        log(`分片 ${index} 已确认 ${digest.slice(0, 12)}…（${formatBytes(size)}）`);
        return;
      } catch (err) {
        // 4xx responses are decisions, not transient failures.  A digest
        // conflict is surfaced to the operator; nothing is overwritten.
        if (err instanceof HttpError && err.status < 500) throw err;
        if (attempt + 1 >= MAX_RETRIES || state.stopped) throw err;
        const delay = 400 * 2 ** attempt;
        log(`分片 ${index} 失败，${delay} ms 后重试（第 ${attempt + 1} 次）: ${describeError(err)}`);
        await sleep(delay);
        if (state.stopped) throw new DOMException("aborted", "AbortError");
      }
    }
  }

  async function uploadAll(missing) {
    const queue = missing.slice();
    const workers = Array.from(
      { length: Math.min(PARALLEL, queue.length) },
      async () => {
        while (queue.length) {
          if (state.stopped) throw new DOMException("aborted", "AbortError");
          const index = queue.shift();
          await uploadChunk(index);
        }
      },
    );
    await Promise.all(workers);
  }

  async function completeNow() {
    let attempt = 0;
    for (;;) {
      try {
        return await api(`/api/uploads/${state.session.session_id}/complete`, {
          method: "POST",
          signal: signal(),
        });
      } catch (err) {
        const retryable = !(err instanceof HttpError) || err.status >= 500;
        attempt += 1;
        if (!retryable || attempt >= MAX_RETRIES || state.stopped) throw err;
        const delay = 500 * 2 ** (attempt - 1);
        log(`完成校验暂时失败，${delay} ms 后重试: ${describeError(err)}`);
        await sleep(delay);
      }
    }
  }

  /**
   * Query confirmed chunks, upload only the missing ones, then complete.
   * Safe to run any number of times; the server is idempotent.
   */
  async function continueUpload() {
    if (!state.session) {
      showError(els.setupError, "请先选择文件并开始上传。");
      return;
    }
    if (state.running) return;
    showError(els.setupError, "");
    showError(els.sessionError, "");
    showNote("");
    setBusy(true);
    state.stopped = false;
    state.controller = new AbortController();
    try {
      const fresh = await refreshSession();
      if (fresh.status === "completed") {
        log("会话已完成，可直接下载。");
        showNote("该会话已完成并校验通过，无需补传。");
        return;
      }

      const missing = fresh.missing_chunks || [];
      if (missing.length) {
        if (!state.file) {
          throw new UiError("请先在上方重新选择同一文件，再继续补传缺失分片。");
        }
        if (state.file.name !== fresh.filename || state.file.size !== fresh.file_size) {
          throw new UiError("所选文件与会话记录不一致，请选择同一文件。");
        }
        log(`查询完成：已确认 ${fresh.received_count}/${fresh.total_chunks} 片，待传 ${missing.length} 片。`);
        await uploadAll(missing);
        if (state.stopped) return;
      } else {
        log(`查询完成：${fresh.total_chunks} 片全部已确认。`);
        showNote("所有分片已在服务端确认，开始整包校验。");
      }

      const result = await completeNow();
      applySession(result);
      log(result.already_completed
        ? "服务端此前已完成登记，沿用同一对象。"
        : `整包校验通过，对象 ${result.object_id} 已登记。`);
      showNote("分片连续、大小与整包 SHA-256 均校验通过，可下载。");
      els.download.focus();
    } catch (err) {
      if (err && err.name === "AbortError") {
        log("操作已停止。");
        showNote("操作已停止，可点击“查询已收分片 / 补传缺片”继续。");
      } else {
        showError(els.sessionError, describeError(err));
        log(describeError(err), "error");
      }
      try { await refreshSession(); } catch (_) { /* keep last view */ }
    } finally {
      setBusy(false);
      state.controller = null;
    }
  }

  // ------------------------------------------------------------------- setup

  function validateSha(value) {
    const digest = value.trim().toLowerCase();
    if (!/^[0-9a-f]{64}$/.test(digest)) {
      showError(els.setupError, "整包 SHA-256 必须是 64 位十六进制字符。");
      return null;
    }
    return digest;
  }

  async function startUpload(event) {
    event.preventDefault();
    if (state.running) return;
    showError(els.setupError, "");
    const file = els.file.files && els.file.files[0];
    if (!file) {
      showError(els.setupError, "请选择要上传的镜像包文件。");
      return;
    }
    const digest = validateSha(els.sha.value);
    if (!digest) return;

    state.file = file;
    els.sha.value = digest;
    setBusy(true);
    let created;
    try {
      const form = new FormData();
      form.append("filename", file.name);
      form.append("file_size", String(file.size));
      form.append("expected_sha256", digest);
      created = await api("/api/uploads", { method: "POST", body: form });
    } catch (err) {
      setBusy(false);
      showError(els.setupError, describeError(err));
      log(describeError(err), "error");
      return;
    }
    applySession(created);
    rememberSession(created);
    setBusy(false);
    if (created.status === "completed") {
      log(`该内容已有完成会话 ${created.session_id}`);
      showNote("该文件此前已上传并校验通过，可直接下载。");
      return;
    }
    log(`会话 ${created.session_id}：已确认 ${created.received_count}/${created.total_chunks} 片。`);
    await continueUpload();
  }

  function reset() {
    if (state.running) {
      state.stopped = true;
      if (state.controller) state.controller.abort();
    }
    state.file = null;
    state.session = null;
    state.received = new Set();
    els.file.value = "";
    els.sha.value = "";
    els.sessionCard.hidden = true;
    els.log.textContent = "";
    showError(els.setupError, "");
    showError(els.sessionError, "");
    showNote("");
    els.file.focus();
  }

  async function restoreLastSession() {
    let id = null;
    try { id = localStorage.getItem(`${STORE_PREFIX}last`); } catch (_) { return; }
    if (!id) return;
    try {
      const session = await api(`/api/uploads/${id}`);
      if (session.status !== "uploading") {
        try { localStorage.removeItem(`${STORE_PREFIX}last`); } catch (_) { /* ignore */ }
        return;
      }
      applySession(session);
      els.sha.value = session.expected_sha256;
      showNote("检测到上次未完成的上传会话。请重新选择同一文件后点击“开始上传 / 继续上传”。");
      log(`恢复上次会话 ${session.session_id}（${session.received_count}/${session.total_chunks} 片）。`);
    } catch (_) {
      try { localStorage.removeItem(`${STORE_PREFIX}last`); } catch (e) { /* ignore */ }
    }
  }

  els.form.addEventListener("submit", startUpload);
  els.reset.addEventListener("click", reset);
  els.resume.addEventListener("click", continueUpload);
  els.complete.addEventListener("click", continueUpload);
  els.cancel.addEventListener("click", () => {
    state.stopped = true;
    if (state.controller) state.controller.abort();
    els.cancel.disabled = true;
    log("正在停止…");
  });
  els.file.addEventListener("change", () => {
    const file = els.file.files && els.file.files[0];
    const s = state.session;
    if (file && s && file.name === s.filename && file.size === s.file_size) {
      state.file = file;
      showNote("已重新选择同一文件，可点击“查询已收分片 / 补传缺片”继续。");
    } else if (file && s) {
      showNote("所选文件与当前会话记录不一致，将按新文件建立或复用会话。");
    } else {
      showNote("");
    }
  });

  setBusy(false);
  restoreLastSession();
})();
