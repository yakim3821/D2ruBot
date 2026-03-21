from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import date, datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import Settings
from .db import Database


ROOT_DIR = Path(__file__).resolve().parents[2]
LOGS_DIR = ROOT_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
DISPLAY_COMMANDS: list[tuple[str, list[str]]] = [
    ("Login Check", ["login-check"]),
    ("Scan Taverna", ["scan-taverna"]),
    ("List New Topics", ["list-new-topics"]),
    ("Sync Yakim Posts", ["sync-yakim-posts", "--pages", "3"]),
    ("Build Yakim Profile", ["build-yakim-profile", "--limit", "500"]),
    ("LLM Draft New Topics", ["llm-draft-new-topics", "--limit", "1"]),
    ("Publish LLM Drafted Topics", ["publish-llm-drafted-topics", "--limit", "1"]),
    ("Publish Daily Summary", ["publish-daily-summary"]),
]
WORKER_COMMAND = ["run-auto-reply-worker"]
DAILY_SUMMARY_WORKER_COMMAND = ["run-daily-summary-worker"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


@dataclass
class ManagedProcess:
    name: str
    command: list[str]
    log_path: Path
    process: subprocess.Popen | None = None
    started_at: str | None = None

    @property
    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    @property
    def pid(self) -> int | None:
        return self.process.pid if self.is_running and self.process else None

    def exit_code(self) -> int | None:
        return None if self.process is None else self.process.poll()


class UIProcessManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._worker = ManagedProcess(
            name="worker",
            command=WORKER_COMMAND,
            log_path=LOGS_DIR / "worker.log",
        )
        self._daily_summary_worker = ManagedProcess(
            name="daily_summary_worker",
            command=DAILY_SUMMARY_WORKER_COMMAND,
            log_path=LOGS_DIR / "publish-daily-summary.log",
        )

    def _spawn(self, command: list[str], log_path: Path) -> subprocess.Popen:
        python_executable = sys.executable
        bot_entry = ROOT_DIR / "bot.py"
        log_path.parent.mkdir(exist_ok=True)
        log_handle = open(log_path, "a", encoding="utf-8")
        timestamp = _utc_now_iso()
        log_handle.write(f"\n[{timestamp}] Starting command: {' '.join(command)}\n")
        log_handle.flush()
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        return subprocess.Popen(
            [python_executable, "-u", str(bot_entry), *command],
            cwd=str(ROOT_DIR),
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def start_worker(self) -> dict[str, Any]:
        with self._lock:
            if self._worker.is_running:
                return {"status": "already_running", "pid": self._worker.pid}
            self._worker.process = self._spawn(self._worker.command, self._worker.log_path)
            self._worker.started_at = _utc_now_iso()
            return {"status": "started", "pid": self._worker.pid}

    def stop_worker(self) -> dict[str, Any]:
        with self._lock:
            if not self._worker.is_running or self._worker.process is None:
                return {"status": "not_running"}
            self._worker.process.terminate()
            try:
                self._worker.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._worker.process.kill()
                self._worker.process.wait(timeout=5)
            return {"status": "stopped", "exit_code": self._worker.exit_code()}

    def run_one_off(self, command: list[str]) -> dict[str, Any]:
        safe_name = "-".join(command).replace("/", "_")
        log_path = LOGS_DIR / f"{safe_name}.log"
        process = self._spawn(command, log_path)
        return {"status": "started", "pid": process.pid, "log": log_path.name}

    def worker_status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "running": self._worker.is_running,
                "pid": self._worker.pid,
                "started_at": self._worker.started_at,
                "log": self._worker.log_path.name,
                "exit_code": self._worker.exit_code(),
            }

    def start_daily_summary_worker(self) -> dict[str, Any]:
        with self._lock:
            if self._daily_summary_worker.is_running:
                return {"status": "already_running", "pid": self._daily_summary_worker.pid}
            self._daily_summary_worker.process = self._spawn(
                self._daily_summary_worker.command,
                self._daily_summary_worker.log_path,
            )
            self._daily_summary_worker.started_at = _utc_now_iso()
            return {"status": "started", "pid": self._daily_summary_worker.pid}

    def stop_daily_summary_worker(self) -> dict[str, Any]:
        with self._lock:
            if not self._daily_summary_worker.is_running or self._daily_summary_worker.process is None:
                return {"status": "not_running"}
            self._daily_summary_worker.process.terminate()
            try:
                self._daily_summary_worker.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._daily_summary_worker.process.kill()
                self._daily_summary_worker.process.wait(timeout=5)
            return {"status": "stopped", "exit_code": self._daily_summary_worker.exit_code()}

    def daily_summary_worker_status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "running": self._daily_summary_worker.is_running,
                "pid": self._daily_summary_worker.pid,
                "started_at": self._daily_summary_worker.started_at,
                "log": self._daily_summary_worker.log_path.name,
                "exit_code": self._daily_summary_worker.exit_code(),
            }


def _tail(path: Path, max_lines: int = 200) -> str:
    if not path.exists():
        return ""
    with open(path, "r", encoding="utf-8", errors="replace") as file:
        lines = file.readlines()
    return "".join(lines[-max_lines:])


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return value


HTML_PAGE = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>D2ruBot Control Panel</title>
  <style>
    :root {
      --bg: #f0ece3;
      --panel: #fffaf0;
      --ink: #1f2a22;
      --muted: #5f6d63;
      --line: #cbbfae;
      --accent: #9a3412;
      --accent-2: #1f6f50;
      --danger: #9f1239;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Trebuchet MS", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(154,52,18,.16), transparent 28%),
        radial-gradient(circle at bottom left, rgba(31,111,80,.16), transparent 30%),
        var(--bg);
    }
    .shell {
      max-width: 1240px;
      margin: 0 auto;
      padding: 28px 18px 40px;
    }
    .hero {
      display: grid;
      gap: 14px;
      margin-bottom: 22px;
      padding: 20px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: linear-gradient(135deg, rgba(255,250,240,.96), rgba(247,239,224,.96));
      box-shadow: 0 16px 40px rgba(60, 40, 20, .08);
    }
    .hero h1 {
      margin: 0;
      font-size: clamp(28px, 4vw, 48px);
      line-height: 1;
    }
    .hero p {
      margin: 0;
      color: var(--muted);
      max-width: 760px;
      font-size: 15px;
    }
    .grid {
      display: grid;
      grid-template-columns: 1.15fr .85fr;
      gap: 18px;
    }
    .card {
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(255,250,240,.95);
      box-shadow: 0 10px 30px rgba(50, 35, 15, .06);
      overflow: hidden;
    }
    .card-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 16px 18px;
      border-bottom: 1px solid var(--line);
    }
    .card-head h2 {
      margin: 0;
      font-size: 18px;
    }
    .card-body {
      padding: 16px 18px 18px;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }
    .metric {
      padding: 14px;
      border: 1px dashed var(--line);
      border-radius: 16px;
      background: rgba(255,255,255,.45);
    }
    .metric .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .08em;
    }
    .metric .value {
      margin-top: 8px;
      font-size: 28px;
      font-weight: 700;
    }
    .actions {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    button {
      appearance: none;
      border: 0;
      border-radius: 14px;
      padding: 12px 14px;
      font: inherit;
      font-weight: 700;
      color: white;
      background: var(--accent);
      cursor: pointer;
      transition: transform .16s ease, opacity .16s ease;
    }
    button.secondary { background: var(--accent-2); }
    button.danger { background: var(--danger); }
    button:hover { transform: translateY(-1px); }
    button:disabled { opacity: .55; cursor: not-allowed; transform: none; }
    .stack {
      display: grid;
      gap: 10px;
    }
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      font-size: 13px;
      background: rgba(31,111,80,.12);
      color: #184d39;
    }
    .status-pill.off {
      background: rgba(159,18,57,.1);
      color: #7b1131;
    }
    pre {
      margin: 0;
      min-height: 360px;
      max-height: 720px;
      overflow: auto;
      padding: 16px;
      border-radius: 16px;
      background: #1c1b19;
      color: #f7f1e6;
      font: 13px/1.45 Consolas, "Courier New", monospace;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
    }
    .toolbar select {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(255,255,255,.7);
      font: inherit;
      color: var(--ink);
    }
    .wide {
      margin-top: 18px;
    }
    .tabbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .tabbtn {
      background: rgba(31,42,34,.08);
      color: var(--ink);
    }
    .tabbtn.active {
      background: var(--accent-2);
      color: white;
    }
    .tabpanel {
      display: none;
    }
    .tabpanel.active {
      display: block;
    }
    .tablewrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255,255,255,.45);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid rgba(203,191,174,.7);
      text-align: left;
      vertical-align: top;
      font-size: 13px;
    }
    th {
      position: sticky;
      top: 0;
      background: #f7efe0;
      z-index: 1;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .06em;
      color: var(--muted);
    }
    td code {
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
    }
    .hint {
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
      .metrics, .actions { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>D2ruBot Control Panel</h1>
      <p>Опциональный UI для ручного управления ботом. CLI и worker продолжают работать отдельно, а этот интерфейс просто запускает команды, показывает логи и статус текущего worker-процесса.</p>
    </section>

    <div class="grid">
      <section class="card">
        <div class="card-head">
          <h2>Статус</h2>
          <button class="secondary" onclick="refreshAll()">Обновить</button>
        </div>
        <div class="card-body stack">
          <div id="worker-pill" class="status-pill off">Worker stopped</div>
          <div class="metrics">
            <div class="metric"><div class="label">Topics Total</div><div id="topics-total" class="value">-</div></div>
            <div class="metric"><div class="label">Ready To Reply</div><div id="topics-ready" class="value">-</div></div>
            <div class="metric"><div class="label">Waiting Delay</div><div id="topics-waiting" class="value">-</div></div>
          </div>
          <div class="metrics">
            <div class="metric"><div class="label">Unreplied</div><div id="topics-unreplied" class="value">-</div></div>
            <div class="metric"><div class="label">Replies Total</div><div id="replies-total" class="value">-</div></div>
            <div class="metric"><div class="label">Auto Published</div><div id="auto-published" class="value">-</div></div>
          </div>
          <div class="hint" id="worker-meta">Worker PID: -, Started: -</div>
        </div>
      </section>

      <section class="card">
        <div class="card-head">
          <h2>Worker Control</h2>
        </div>
        <div class="card-body actions">
          <button class="secondary" onclick="startWorker()">Start Worker</button>
          <button class="danger" onclick="stopWorker()">Stop Worker</button>
        </div>
      </section>

      <section class="card">
        <div class="card-head">
          <h2>Daily Summary</h2>
        </div>
        <div class="card-body stack">
          <div id="daily-summary-pill" class="status-pill off">Daily summary worker stopped</div>
          <label><input id="daily-summary-enabled" type="checkbox"> Автопубликация включена</label>
          <div class="toolbar">
            <input id="daily-summary-time" type="time" value="12:00">
            <button class="secondary" onclick="saveDailySummarySchedule()">Save Schedule</button>
            <button onclick="runDailySummaryNow()">Run Now</button>
          </div>
          <div class="hint" id="daily-summary-meta">Worker PID: -, Started: -</div>
          <div class="hint" id="daily-summary-last">Последний запуск: -</div>
        </div>
      </section>

      <section class="card">
        <div class="card-head">
          <h2>Commands</h2>
        </div>
        <div class="card-body actions" id="commands"></div>
      </section>

      <section class="card">
        <div class="card-head">
          <h2>Logs</h2>
          <div class="toolbar">
            <select id="log-select" onchange="refreshLogs()"></select>
            <button class="secondary" onclick="refreshLogs()">Reload Log</button>
          </div>
        </div>
        <div class="card-body">
          <pre id="log-output">Loading...</pre>
        </div>
      </section>
    </div>

    <section class="card wide">
      <div class="card-head">
        <h2>Monitoring Data</h2>
        <div class="tabbar">
          <button class="tabbtn active" data-tab="waiting" onclick="selectTab('waiting')">Ожидание</button>
          <button class="tabbtn" data-tab="ready" onclick="selectTab('ready')">Готово к ответу</button>
          <button class="tabbtn" data-tab="replies" onclick="selectTab('replies')">Последние ответы</button>
          <button class="tabbtn" data-tab="failures" onclick="selectTab('failures')">Ошибки</button>
        </div>
      </div>
      <div class="card-body">
        <div id="tab-waiting" class="tabpanel active">
          <div class="tablewrap"><table id="waiting-table"></table></div>
        </div>
        <div id="tab-ready" class="tabpanel">
          <div class="tablewrap"><table id="ready-table"></table></div>
        </div>
        <div id="tab-replies" class="tabpanel">
          <div class="tablewrap"><table id="replies-table"></table></div>
        </div>
        <div id="tab-failures" class="tabpanel">
          <div class="tablewrap"><table id="failures-table"></table></div>
        </div>
      </div>
    </section>
  </div>

  <script>
    const COMMANDS = __COMMANDS__;
    const logs = ["worker.log", "publish-daily-summary.log", ...COMMANDS.map(x => x.log)];

    function renderCommands() {
      const box = document.getElementById("commands");
      const logSelect = document.getElementById("log-select");
      box.innerHTML = "";
      logSelect.innerHTML = "";
      for (const name of logs) {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name;
        logSelect.appendChild(option);
      }
      for (const cmd of COMMANDS) {
        const btn = document.createElement("button");
        btn.textContent = cmd.label;
        btn.onclick = async () => {
          await fetchJson("/api/command", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({command: cmd.command})
          });
          document.getElementById("log-select").value = cmd.log;
          await refreshLogs();
          await refreshStatus();
        };
        box.appendChild(btn);
      }
    }

    async function fetchJson(url, options) {
      const response = await fetch(url, options);
      if (!response.ok) {
        throw new Error(await response.text());
      }
      return response.json();
    }

    async function refreshStatus() {
      const data = await fetchJson("/api/status");
      document.getElementById("topics-total").textContent = data.dashboard.topics_total ?? "-";
      document.getElementById("topics-ready").textContent = data.dashboard.topics_ready_to_reply ?? "-";
      document.getElementById("topics-waiting").textContent = data.dashboard.topics_waiting_delay ?? "-";
      document.getElementById("topics-unreplied").textContent = data.dashboard.topics_unreplied ?? "-";
      document.getElementById("replies-total").textContent = data.dashboard.bot_replies_total ?? "-";
      document.getElementById("auto-published").textContent = data.dashboard.bot_auto_published ?? "-";
      const pill = document.getElementById("worker-pill");
      pill.className = data.worker.running ? "status-pill" : "status-pill off";
      pill.textContent = data.worker.running ? "Worker running" : "Worker stopped";
      document.getElementById("worker-meta").textContent =
        `Worker PID: ${data.worker.pid ?? "-"}, Started: ${data.worker.started_at ?? "-"}`;
      const summaryPill = document.getElementById("daily-summary-pill");
      summaryPill.className = data.daily_summary.worker.running ? "status-pill" : "status-pill off";
      summaryPill.textContent = data.daily_summary.worker.running
        ? "Daily summary worker running"
        : "Daily summary worker stopped";
      document.getElementById("daily-summary-enabled").checked = !!data.daily_summary.schedule.enabled;
      document.getElementById("daily-summary-time").value = data.daily_summary.schedule.schedule_time ?? "12:00";
      document.getElementById("daily-summary-meta").textContent =
        `Worker PID: ${data.daily_summary.worker.pid ?? "-"}, Started: ${data.daily_summary.worker.started_at ?? "-"}`;
      const lastRun = data.daily_summary.latest_run;
      document.getElementById("daily-summary-last").textContent = lastRun
        ? `Последний запуск: ${lastRun.summary_date} | ${lastRun.status} | ${lastRun.topic_url ?? "-"}`
        : "Последний запуск: -";
    }

    async function refreshLogs() {
      const selected = document.getElementById("log-select").value || "worker.log";
      const data = await fetchJson(`/api/logs?name=${encodeURIComponent(selected)}`);
      document.getElementById("log-output").textContent = data.content || "(empty log)";
    }

    function renderTable(elementId, columns, rows) {
      const table = document.getElementById(elementId);
      if (!rows || rows.length === 0) {
        table.innerHTML = "<tr><td>No data</td></tr>";
        return;
      }
      const thead = `<thead><tr>${columns.map(col => `<th>${col.label}</th>`).join("")}</tr></thead>`;
      const tbody = `<tbody>${rows.map(row => `<tr>${columns.map(col => `<td>${col.render ? col.render(row[col.key], row) : (row[col.key] ?? "")}</td>`).join("")}</tr>`).join("")}</tbody>`;
      table.innerHTML = `${thead}${tbody}`;
    }

    async function refreshMonitoring() {
      const data = await fetchJson("/api/monitor");
      renderTable("waiting-table", [
        {key: "forum_topic_id", label: "Topic ID"},
        {key: "title", label: "Title"},
        {key: "forum_reply_count", label: "Replies"},
        {key: "reply_not_before", label: "Reply Not Before"},
        {key: "topic_url", label: "URL", render: (v) => `<a href="${v}" target="_blank" rel="noreferrer">open</a>`}
      ], data.waiting_topics);
      renderTable("ready-table", [
        {key: "forum_topic_id", label: "Topic ID"},
        {key: "title", label: "Title"},
        {key: "forum_reply_count", label: "Replies"},
        {key: "reply_not_before", label: "Reply Not Before"},
        {key: "topic_url", label: "URL", render: (v) => `<a href="${v}" target="_blank" rel="noreferrer">open</a>`}
      ], data.ready_topics);
      renderTable("replies-table", [
        {key: "created_at", label: "Created"},
        {key: "forum_topic_id", label: "Topic ID"},
        {key: "title", label: "Title"},
        {key: "status", label: "Status"},
        {key: "reply_preview", label: "Reply Preview"},
        {key: "target_url", label: "URL", render: (v) => `<a href="${v}" target="_blank" rel="noreferrer">open</a>`}
      ], data.recent_replies);
      renderTable("failures-table", [
        {key: "created_at", label: "Created"},
        {key: "forum_topic_id", label: "Topic ID"},
        {key: "title", label: "Title"},
        {key: "status", label: "Status"},
        {key: "error_message", label: "Error"}
      ], data.recent_failures);
    }

    function selectTab(name) {
      document.querySelectorAll(".tabbtn").forEach(el => el.classList.toggle("active", el.dataset.tab === name));
      document.querySelectorAll(".tabpanel").forEach(el => el.classList.toggle("active", el.id === `tab-${name}`));
    }

    async function startWorker() {
      await fetchJson("/api/worker/start", {method: "POST"});
      document.getElementById("log-select").value = "worker.log";
      await refreshAll();
    }

    async function stopWorker() {
      await fetchJson("/api/worker/stop", {method: "POST"});
      await refreshAll();
    }

    async function saveDailySummarySchedule() {
      const enabled = document.getElementById("daily-summary-enabled").checked;
      const scheduleTime = document.getElementById("daily-summary-time").value || "12:00";
      await fetchJson("/api/daily-summary/config", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({enabled, schedule_time: scheduleTime})
      });
      await refreshAll();
    }

    async function runDailySummaryNow() {
      await fetchJson("/api/daily-summary/run", {method: "POST"});
      document.getElementById("log-select").value = "publish-daily-summary.log";
      await refreshAll();
    }

    async function refreshAll() {
      await refreshStatus();
      await refreshLogs();
      await refreshMonitoring();
    }

    renderCommands();
    refreshAll();
    setInterval(refreshStatus, 5000);
    setInterval(refreshLogs, 5000);
    setInterval(refreshMonitoring, 10000);
  </script>
</body>
</html>
"""


class BotUI:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.db_settings())
        self.manager = UIProcessManager()
        schedule = self.db.get_daily_summary_schedule()
        if schedule.get("enabled"):
            self.manager.start_daily_summary_worker()

    def command_specs(self) -> list[dict[str, Any]]:
        specs = []
        for label, command in DISPLAY_COMMANDS:
            specs.append(
                {
                    "label": label,
                    "command": command,
                    "log": f"{'-'.join(command).replace('/', '_')}.log",
                }
            )
        return specs

    def status(self) -> dict[str, Any]:
        return {
            "worker": self.manager.worker_status(),
            "dashboard": self.db.get_dashboard_status(),
            "daily_summary": {
                "worker": self.manager.daily_summary_worker_status(),
                "schedule": self.db.get_daily_summary_schedule(),
                "latest_run": (self.db.get_recent_daily_summary_runs(limit=1) or [None])[0],
            },
        }

    def monitoring(self) -> dict[str, Any]:
        return {
            "waiting_topics": self.db.get_waiting_topics(limit=50),
            "ready_topics": self.db.get_ready_topics(limit=50),
            "recent_replies": self.db.get_recent_bot_replies(limit=50),
            "recent_failures": self.db.get_recent_failures(limit=50),
            "recent_daily_summaries": self.db.get_recent_daily_summary_runs(limit=20),
        }

    def run_command(self, command: list[str]) -> dict[str, Any]:
        return self.manager.run_one_off(command)

    def start_worker(self) -> dict[str, Any]:
        return self.manager.start_worker()

    def stop_worker(self) -> dict[str, Any]:
        return self.manager.stop_worker()

    def update_daily_summary_schedule(self, enabled: bool, schedule_time: str) -> dict[str, Any]:
        self.db.set_daily_summary_schedule(enabled=enabled, schedule_time=schedule_time)
        if enabled:
            worker = self.manager.start_daily_summary_worker()
        else:
            worker = self.manager.stop_daily_summary_worker()
        return {
            "schedule": self.db.get_daily_summary_schedule(),
            "worker": self.manager.daily_summary_worker_status(),
            "action": worker,
        }

    def run_daily_summary_now(self) -> dict[str, Any]:
        if self.manager.daily_summary_worker_status().get("running"):
            return {
                "status": "worker_running",
                "message": "Daily summary worker is already running. Stop it before starting a manual run.",
            }
        return self.manager.run_one_off(["publish-daily-summary"])

    def read_log(self, name: str) -> dict[str, Any]:
        safe_name = Path(name).name
        return {"name": safe_name, "content": _tail(LOGS_DIR / safe_name)}


class UIRequestHandler(BaseHTTPRequestHandler):
    ui: BotUI

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(HTML_PAGE.replace("__COMMANDS__", json.dumps(self.ui.command_specs(), ensure_ascii=False)))
            return
        if parsed.path == "/api/status":
            self._send_json(self.ui.status())
            return
        if parsed.path == "/api/monitor":
            self._send_json(self.ui.monitoring())
            return
        if parsed.path == "/api/logs":
            params = parse_qs(parsed.query)
            name = params.get("name", ["worker.log"])[0]
            self._send_json(self.ui.read_log(name))
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        body = self._read_json()
        if parsed.path == "/api/worker/start":
            self._send_json(self.ui.start_worker())
            return
        if parsed.path == "/api/worker/stop":
            self._send_json(self.ui.stop_worker())
            return
        if parsed.path == "/api/daily-summary/config":
            enabled = bool(body.get("enabled"))
            schedule_time = str(body.get("schedule_time") or "12:00")
            self._send_json(self.ui.update_daily_summary_schedule(enabled=enabled, schedule_time=schedule_time))
            return
        if parsed.path == "/api/daily-summary/run":
            self._send_json(self.ui.run_daily_summary_now())
            return
        if parsed.path == "/api/command":
            command = body.get("command")
            if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
                self.send_error(HTTPStatus.BAD_REQUEST, "command must be a list of strings")
                return
            self._send_json(self.ui.run_command(command))
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def log_message(self, fmt: str, *args) -> None:
        return

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, payload: dict[str, Any]) -> None:
        raw = json.dumps(_json_ready(payload), ensure_ascii=False).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, html: str) -> None:
        raw = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def run_ui_server(settings: Settings, host: str = "127.0.0.1", port: int = 8080) -> None:
    ui = BotUI(settings)
    handler = type("BotUIHandler", (UIRequestHandler,), {"ui": ui})
    server = ThreadingHTTPServer((host, port), handler)
    print(f"UI server started on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("UI server stopped.")
    finally:
        server.server_close()
