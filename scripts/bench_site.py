#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import re
from pathlib import Path


METRIC_DEFINITIONS = (
    {
        "key": "throughput_bytes_per_sec",
        "label": "Throughput",
        "unit": "B/s",
        "direction": "higher",
    },
    {
        "key": "latency_p50_ns",
        "label": "Latency p50",
        "unit": "ns",
        "direction": "lower",
    },
    {
        "key": "latency_p95_ns",
        "label": "Latency p95",
        "unit": "ns",
        "direction": "lower",
    },
    {
        "key": "latency_p99_ns",
        "label": "Latency p99",
        "unit": "ns",
        "direction": "lower",
    },
    {
        "key": "serialize_elapsed_ns",
        "label": "Serialize",
        "unit": "ns",
        "direction": "lower",
    },
    {
        "key": "deserialize_elapsed_ns",
        "label": "Deserialize",
        "unit": "ns",
        "direction": "lower",
    },
)


def read_results(path):
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def safe_component(value):
    component = re.sub(r"[^A-Za-z0-9._-]", "-", value)
    if not component or component in {".", ".."}:
        raise ValueError(f"不正なパス要素: {value}")
    return component


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def parse_rfc3339(value):
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value)


def run_document(
    metadata,
    cases,
    results,
    commit,
    commit_message,
    run_id,
    measured_at,
    run_url,
    artifact_url,
    commit_url,
):
    return {
        "schema_version": 1,
        "run_id": run_id,
        "commit": commit,
        "commit_message": commit_message,
        "commit_url": commit_url,
        "measured_at": measured_at,
        "measured_at_unix": int(parse_rfc3339(measured_at).timestamp()),
        "run_url": run_url,
        "artifact_url": artifact_url,
        "metadata": metadata,
        "cases": cases,
        "results": results,
    }


def discover_runs(site_dir):
    runs = []
    for path in sorted((site_dir / "results").glob("*/runs/*.json")):
        try:
            runs.append((path, json.loads(path.read_text(encoding="utf-8"))))
        except (OSError, json.JSONDecodeError):
            continue
    if runs:
        return runs
    for path in sorted((site_dir / "results").glob("*/arm64.json")):
        try:
            runs.append((path, json.loads(path.read_text(encoding="utf-8"))))
        except (OSError, json.JSONDecodeError):
            continue
    return runs


def metadata_scalar(value):
    if isinstance(value, dict):
        return value.get("value")
    return value


def index_entry(path, document, site_dir):
    results = document.get("results", [])
    transports = sorted({result.get("transport") for result in results if result.get("transport")})
    cases = sorted({result.get("case_id") for result in results if result.get("case_id")})
    statuses = sorted({result.get("status") for result in results if result.get("status")})
    vendor = document.get("metadata", {}).get("vendor", {})
    metrics = []
    for result in results:
        latency = result.get("latency") or {}
        metrics.append(
            {
                "case_id": result.get("case_id"),
                "transport": result.get("transport"),
                "status": result.get("status"),
                "throughput_bytes_per_sec": result.get("throughput_bytes_per_sec"),
                "latency_p50_ns": latency.get("p50_ns"),
                "latency_p95_ns": latency.get("p95_ns"),
                "latency_p99_ns": latency.get("p99_ns"),
                "serialize_elapsed_ns": result.get("serialize_elapsed_ns"),
                "deserialize_elapsed_ns": result.get("deserialize_elapsed_ns"),
            }
        )
    return {
        "run_id": document.get("run_id"),
        "commit": document.get("commit"),
        "commit_message": document.get("commit_message"),
        "commit_url": document.get("commit_url"),
        "measured_at": document.get("measured_at"),
        "measured_at_unix": document.get("measured_at_unix", 0),
        "transports": transports,
        "cases": cases,
        "statuses": statuses,
        "cyclonedds_sha": metadata_scalar(vendor.get("cyclonedds_sha")),
        "metrics": metrics,
        "result_path": path.relative_to(site_dir).as_posix(),
        "run_url": document.get("run_url"),
        "artifact_url": document.get("artifact_url"),
    }


def build_index(site_dir):
    entries = [index_entry(path, document, site_dir) for path, document in discover_runs(site_dir)]
    entries.sort(key=lambda entry: (entry["measured_at_unix"], entry["run_id"] or ""), reverse=True)
    write_json(
        site_dir / "index.json",
        {
            "schema_version": 1,
            "metric_catalog": list(METRIC_DEFINITIONS),
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "runs": entries,
        },
    )


def write_index_html(site_dir):
    document = """<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CycloneDDS Rust ベンチマーク履歴</title>
  <style>
    :root {
      color-scheme: light dark;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      --background: #f7f8fa;
      --surface: #ffffff;
      --surface-muted: #eef1f5;
      --border: #d5dbe3;
      --text: #17202b;
      --muted: #5d6875;
      --accent: #2563eb;
      --accent-muted: #dbeafe;
      --good: #187b35;
      --bad: #b3261e;
      --line: #2563eb;
      --grid: #d5dbe3;
    }
    @media (prefers-color-scheme: dark) {
      :root {
        --background: #12161c;
        --surface: #1b222c;
        --surface-muted: #252e3a;
        --border: #3b4654;
        --text: #eef2f7;
        --muted: #aeb8c5;
        --accent-muted: #1e3a8a;
        --grid: #3b4654;
      }
    }
    * { box-sizing: border-box; }
    body { background: var(--background); color: var(--text); margin: 0; }
    main { margin: 0 auto; max-width: 1320px; padding: 2rem 1rem 4rem; }
    header { align-items: end; display: flex; gap: 1rem; justify-content: space-between; margin-bottom: 1.5rem; }
    h1, h2, p { margin-top: 0; }
    h1 { font-size: clamp(1.5rem, 3vw, 2.2rem); margin-bottom: .35rem; }
    h2 { font-size: 1.1rem; margin-bottom: 1rem; }
    .subtitle, .muted { color: var(--muted); }
    .panel { background: var(--surface); border: 1px solid var(--border); border-radius: .7rem; box-shadow: 0 2px 8px #0000000d; margin-bottom: 1rem; padding: 1rem; }
    form { display: grid; gap: .8rem; grid-template-columns: repeat(auto-fit, minmax(12rem, 1fr)); }
    label { color: var(--muted); display: grid; font-size: .9rem; gap: .3rem; }
    input, select, button { background: var(--surface); border: 1px solid var(--border); border-radius: .35rem; color: var(--text); font: inherit; min-height: 2.2rem; padding: .4rem .55rem; }
    button { cursor: pointer; }
    button:hover { border-color: var(--accent); }
    .summary-grid { display: grid; gap: .8rem; grid-template-columns: repeat(auto-fit, minmax(12rem, 1fr)); margin-bottom: 1rem; }
    .summary-card { background: var(--surface); border: 1px solid var(--border); border-radius: .7rem; min-height: 6.4rem; padding: .9rem 1rem; }
    .summary-card span { color: var(--muted); display: block; font-size: .85rem; margin-bottom: .35rem; }
    .summary-card strong { display: block; font-size: 1.25rem; overflow-wrap: anywhere; }
    .summary-card a { color: var(--accent); }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    .neutral { color: var(--muted); }
    .chart-header { align-items: baseline; display: flex; flex-wrap: wrap; gap: .75rem 1.5rem; justify-content: space-between; }
    .chart-header p { margin-bottom: 0; }
    .chart-scroll { overflow-x: auto; }
    #chart { display: block; height: auto; min-width: 720px; width: 100%; }
    #chart text { fill: var(--muted); font-size: 12px; }
    #chart .axis-label { fill: var(--text); font-size: 13px; }
    #chart .grid-line { stroke: var(--grid); stroke-dasharray: 3 5; stroke-width: 1; }
    #chart .series-line { fill: none; stroke: var(--line); stroke-linecap: round; stroke-linejoin: round; stroke-width: 3; }
    #chart .point { cursor: pointer; fill: var(--surface); stroke: var(--line); stroke-width: 3; }
    #chart .point:hover { fill: var(--accent-muted); r: 7; }
    #chart-empty { color: var(--muted); display: none; padding: 5rem 1rem; text-align: center; }
    .chart-footer { align-items: center; display: flex; flex-wrap: wrap; gap: .75rem; justify-content: space-between; margin-top: .5rem; }
    .tooltip { background: var(--text); border-radius: .35rem; color: var(--background); display: none; max-width: 20rem; padding: .65rem .75rem; pointer-events: none; position: absolute; z-index: 2; }
    .tooltip strong, .tooltip span { display: block; }
    .tooltip span { font-size: .85rem; margin-top: .2rem; }
    .chart-area { position: relative; }
    .table-scroll { overflow-x: auto; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid var(--border); padding: .6rem .5rem; text-align: left; vertical-align: top; }
    th { color: var(--muted); font-size: .85rem; font-weight: 600; white-space: nowrap; }
    td { overflow-wrap: anywhere; }
    a { color: var(--accent); }
    code { overflow-wrap: anywhere; }
    .status-ok { color: var(--good); }
    .status-error { color: var(--bad); }
    .links { display: flex; flex-wrap: wrap; gap: .5rem; }
    @media (max-width: 640px) {
      header { align-items: start; flex-direction: column; }
      main { padding-top: 1.25rem; }
      th, td { padding: .5rem .35rem; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>CycloneDDS Rust ベンチマーク履歴</h1>
        <p class="subtitle">コミットごとの性能変化を確認できます。閾値による自動判定は行いません。</p>
      </div>
      <div class="muted">最終更新: <time id="updated-at">-</time></div>
    </header>

    <section class="panel">
      <h2>表示条件</h2>
      <form id="filters">
        <label>ケース<select id="case"></select></label>
        <label>メトリクス<select id="metric"></select></label>
        <label>Transport<select id="transport"><option value="">すべて</option></select></label>
        <label>コミット<input id="commit" type="search" placeholder="SHA の先頭文字列"></label>
        <label>日付<input id="date" type="date"></label>
      </form>
    </section>

    <section class="summary-grid" aria-label="ベンチマーク概要">
      <article class="summary-card"><span>最新値</span><strong id="latest-value">-</strong></article>
      <article class="summary-card"><span>前回からの変化</span><strong id="latest-delta">-</strong></article>
      <article class="summary-card"><span>表示点数</span><strong id="point-count">-</strong></article>
      <article class="summary-card"><span>最新コミット</span><strong id="latest-commit">-</strong></article>
    </section>

    <section class="panel">
      <div class="chart-header">
        <div>
          <h2 id="chart-title">-</h2>
          <p id="chart-description" class="muted">-</p>
        </div>
        <button id="download" type="button">表示データを JSON で保存</button>
      </div>
      <div class="chart-area">
        <div id="tooltip" class="tooltip" role="status"></div>
        <div class="chart-scroll">
          <svg id="chart" viewBox="0 0 960 400" role="img" aria-labelledby="chart-title"></svg>
        </div>
        <div id="chart-empty">条件に一致する数値がありません。</div>
      </div>
      <div class="chart-footer"><span class="muted">横軸: 実行時刻（点を選択すると詳細を表示）</span><span id="direction" class="muted">-</span></div>
    </section>

    <section class="panel">
      <h2>選択した履歴</h2>
      <div class="table-scroll">
        <table>
          <thead><tr><th>日時</th><th>コミット</th><th>メッセージ</th><th>値</th><th>前回比</th><th>状態</th><th>リンク</th></tr></thead>
          <tbody id="runs"></tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    const CHART_LIMIT = 100;
    const SVG_NS = "http://www.w3.org/2000/svg";
    const state = { runs: [], metricCatalog: [], caseId: "", metricKey: "", indexUpdatedAt: "" };
    const element = id => document.getElementById(id);
    const setText = (id, value) => { element(id).textContent = value; };
    const createSvg = (name, attributes = {}) => {
      const node = document.createElementNS(SVG_NS, name);
      for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
      return node;
    };
    const addLink = (parent, href, label) => {
      if (!href) return;
      const anchor = document.createElement("a");
      anchor.href = href;
      anchor.textContent = label;
      anchor.target = "_blank";
      anchor.rel = "noopener noreferrer";
      parent.append(anchor);
    };
    const shortCommit = commit => (commit || "-").slice(0, 8);
    const numeric = value => {
      const result = Number(value);
      return Number.isFinite(result) ? result : null;
    };
    const metricDefinition = () => state.metricCatalog.find(metric => metric.key === state.metricKey) || null;
    const metricRecord = run => {
      const records = run.metrics || [];
      return records.find(record => record.case_id === state.caseId
        && (!element("transport").value || record.transport === element("transport").value)
        && numeric(record[state.metricKey]) !== null) || null;
    };
    const filteredRuns = () => {
      const commit = element("commit").value.trim().toLowerCase();
      const date = element("date").value;
      const transport = element("transport").value;
      return state.runs.filter(run => (!commit || (run.commit || "").toLowerCase().startsWith(commit))
        && (!date || (run.measured_at || "").startsWith(date))
        && (!transport || (run.transports || []).includes(transport))
        && (!state.caseId || (run.cases || []).includes(state.caseId)));
    };
    const chartPoints = () => filteredRuns()
      .map(run => {
        const record = metricRecord(run);
        if (!record) return null;
        return { run, record, value: numeric(record[state.metricKey]) };
      })
      .filter(point => point !== null)
      .sort((left, right) => (left.run.measured_at_unix - right.run.measured_at_unix)
        || (left.run.run_id || "").localeCompare(right.run.run_id || ""));
    const formatNumber = value => new Intl.NumberFormat("ja-JP", { maximumSignificantDigits: 6 }).format(value);
    const formatValue = (value, metric) => `${formatNumber(value)} ${metric.unit}`;
    const formatDate = value => {
      if (!value) return "-";
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? value : date.toLocaleString("ja-JP", { dateStyle: "medium", timeStyle: "short" });
    };
    const formatDelta = (current, previous) => {
      if (previous === null || previous === 0 || current === null) return null;
      return ((current - previous) / Math.abs(previous)) * 100;
    };
    const deltaText = (delta, metric) => {
      if (delta === null) return { text: "前回値なし", className: "neutral" };
      const sign = delta > 0 ? "+" : "";
      const improved = metric.direction === "higher" ? delta > 0 : delta < 0;
      const className = delta === 0 ? "neutral" : (improved ? "good" : "bad");
      return { text: `${sign}${delta.toFixed(2)}%`, className };
    };
    const clear = node => { while (node.firstChild) node.removeChild(node.firstChild); };
    const linkGroup = (run, resultPath) => {
      const wrapper = document.createElement("span");
      wrapper.className = "links";
      addLink(wrapper, run.commit_url, "commit");
      addLink(wrapper, run.run_url, "workflow");
      addLink(wrapper, run.artifact_url, "artifact");
      addLink(wrapper, resultPath, "results");
      return wrapper;
    };
    const pointsForDisplay = () => chartPoints().slice(-CHART_LIMIT);
    function updateMetricOptions() {
      const current = state.metricKey;
      const transport = element("transport").value;
      const available = state.metricCatalog.filter(metric => state.runs.some(run => {
        if (state.caseId && !(run.cases || []).includes(state.caseId)) return false;
        if (transport && !(run.transports || []).includes(transport)) return false;
        return (run.metrics || []).some(record => record.case_id === state.caseId
          && (!transport || record.transport === transport)
          && numeric(record[metric.key]) !== null);
      }));
      const select = element("metric");
      clear(select);
      for (const metric of available) {
        const option = document.createElement("option");
        option.value = metric.key;
        option.textContent = `${metric.label} (${metric.unit})`;
        select.append(option);
      }
      state.metricKey = available.some(metric => metric.key === current) ? current : (available[0]?.key || "");
      select.value = state.metricKey;
    }
    function showTooltip(event, point, metric) {
      const tooltip = element("tooltip");
      clear(tooltip);
      const title = document.createElement("strong");
      title.textContent = `${shortCommit(point.run.commit)} · ${formatValue(point.value, metric)}`;
      const date = document.createElement("span");
      date.textContent = formatDate(point.run.measured_at);
      tooltip.append(title, date);
      if (point.run.commit) {
        const commit = document.createElement("span");
        commit.textContent = point.run.commit;
        tooltip.append(commit);
      }
      if (point.run.commit_message) {
        const message = document.createElement("span");
        message.textContent = point.run.commit_message;
        tooltip.append(message);
      }
      const chartArea = document.querySelector(".chart-area").getBoundingClientRect();
      tooltip.style.left = `${event.clientX - chartArea.left + 14}px`;
      tooltip.style.top = `${event.clientY - chartArea.top + 14}px`;
      tooltip.style.display = "block";
    }
    function hideTooltip() { element("tooltip").style.display = "none"; }
    function renderChart(points, metric) {
      const chart = element("chart");
      clear(chart);
      const width = 960;
      const height = 400;
      const margin = { top: 24, right: 24, bottom: 72, left: 86 };
      if (!points.length || !metric) {
        chart.style.display = "none";
        element("chart-empty").style.display = "block";
        return;
      }
      chart.style.display = "block";
      element("chart-empty").style.display = "none";
      const values = points.map(point => point.value);
      let minimum = Math.min(...values);
      let maximum = Math.max(...values);
      const spread = maximum - minimum;
      const padding = spread === 0 ? Math.max(Math.abs(maximum) * .05, 1) : spread * .1;
      minimum -= padding;
      maximum += padding;
      const plotWidth = width - margin.left - margin.right;
      const plotHeight = height - margin.top - margin.bottom;
      const x = index => points.length === 1 ? margin.left + plotWidth / 2 : margin.left + (plotWidth * index) / (points.length - 1);
      const y = value => margin.top + plotHeight - ((value - minimum) / (maximum - minimum)) * plotHeight;
      for (let index = 0; index <= 4; index += 1) {
        const value = minimum + ((maximum - minimum) * index) / 4;
        const yPosition = y(value);
        chart.append(createSvg("line", { x1: margin.left, x2: width - margin.right, y1: yPosition, y2: yPosition, class: "grid-line" }));
        const label = createSvg("text", { x: margin.left - 10, y: yPosition + 4, "text-anchor": "end" });
        label.textContent = formatValue(value, metric);
        chart.append(label);
      }
      const line = createSvg("polyline", { points: points.map((point, index) => `${x(index)},${y(point.value)}`).join(" "), class: "series-line" });
      chart.append(line);
      points.forEach((point, index) => {
        const circle = createSvg("circle", { cx: x(index), cy: y(point.value), r: 5, class: "point" });
        circle.addEventListener("pointerenter", event => showTooltip(event, point, metric));
        circle.addEventListener("pointermove", event => showTooltip(event, point, metric));
        circle.addEventListener("pointerleave", hideTooltip);
        circle.addEventListener("click", () => {
          const href = point.run.commit_url || point.run.run_url;
          if (href) window.open(href, "_blank", "noopener,noreferrer");
        });
        chart.append(circle);
        if (index === 0 || index === points.length - 1 || index % Math.max(1, Math.ceil(points.length / 8)) === 0) {
          const label = createSvg("text", { x: x(index), y: height - margin.bottom + 22, "text-anchor": "middle", transform: `rotate(-32 ${x(index)} ${height - margin.bottom + 22})` });
          label.textContent = shortCommit(point.run.commit);
          chart.append(label);
        }
      });
      const xLabel = createSvg("text", { x: margin.left + plotWidth / 2, y: height - 8, class: "axis-label", "text-anchor": "middle" });
      xLabel.textContent = "コミット";
      chart.append(xLabel);
    }
    function renderSummary(points, metric) {
      setText("point-count", points.length ? `${points.length} / ${chartPoints().length}` : "0");
      setText("direction", metric ? (metric.direction === "higher" ? "大きいほど良い" : "小さいほど良い") : "-");
      if (!points.length || !metric) {
        setText("latest-value", "-");
        setText("latest-delta", "-");
        element("latest-delta").className = "neutral";
        clear(element("latest-commit"));
        setText("latest-commit", "-");
        return;
      }
      const latest = points[points.length - 1];
      const previous = points.length > 1 ? points[points.length - 2].value : null;
      const delta = deltaText(formatDelta(latest.value, previous), metric);
      setText("latest-value", formatValue(latest.value, metric));
      setText("latest-delta", delta.text);
      element("latest-delta").className = delta.className;
      clear(element("latest-commit"));
      addLink(element("latest-commit"), latest.run.commit_url || latest.run.run_url, shortCommit(latest.run.commit));
      if (!latest.run.commit_url && !latest.run.run_url) setText("latest-commit", shortCommit(latest.run.commit));
    }
    function renderTable(points, metric) {
      const tbody = element("runs");
      clear(tbody);
      for (let index = points.length - 1; index >= 0; index -= 1) {
        const point = points[index];
        const row = document.createElement("tr");
        const date = document.createElement("td");
        date.textContent = formatDate(point.run.measured_at);
        const commit = document.createElement("td");
        commit.textContent = shortCommit(point.run.commit);
        commit.title = point.run.commit || "";
        const message = document.createElement("td");
        message.textContent = point.run.commit_message || "-";
        const value = document.createElement("td");
        value.textContent = formatValue(point.value, metric);
        const delta = document.createElement("td");
        const previous = index > 0 ? points[index - 1].value : null;
        const deltaValue = deltaText(formatDelta(point.value, previous), metric);
        delta.textContent = deltaValue.text;
        delta.className = deltaValue.className;
        const status = document.createElement("td");
        status.textContent = point.record.status || "-";
        status.className = point.record.status === "ok" ? "status-ok" : "status-error";
        const links = document.createElement("td");
        links.append(linkGroup(point.run, point.run.result_path));
        row.append(date, commit, message, value, delta, status, links);
        tbody.append(row);
      }
      if (!points.length) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 7;
        cell.className = "muted";
        cell.textContent = "表示できる履歴がありません。";
        row.append(cell);
        tbody.append(row);
      }
    }
    function render() {
      updateMetricOptions();
      const metric = metricDefinition();
      const points = pointsForDisplay();
      const caseLabel = state.caseId || "-";
      setText("chart-title", metric ? `${caseLabel} · ${metric.label}` : caseLabel);
      setText("chart-description", metric ? `${metric.unit} / ${metric.direction === "higher" ? "大きいほど良い" : "小さいほど良い"}` : "-");
      renderSummary(points, metric);
      renderChart(points, metric);
      renderTable(points, metric);
    }
    function populateFilters(index) {
      const cases = [...new Set((index.runs || []).flatMap(run => run.cases || []))].sort();
      const transports = [...new Set((index.runs || []).flatMap(run => run.transports || []))].sort();
      const caseSelect = element("case");
      for (const value of cases) {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        caseSelect.append(option);
      }
      state.caseId = cases[0] || "";
      for (const value of transports) {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        element("transport").append(option);
      }
      caseSelect.value = state.caseId;
    }
    function downloadSelected() {
      const metric = metricDefinition();
      const points = pointsForDisplay();
      const payload = { case_id: state.caseId, metric, points: points.map(point => ({ commit: point.run.commit, commit_message: point.run.commit_message, measured_at: point.run.measured_at, value: point.value, status: point.record.status })) };
      const url = URL.createObjectURL(new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" }));
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `${state.caseId || "benchmark"}-${state.metricKey || "data"}.json`;
      anchor.click();
      setTimeout(() => URL.revokeObjectURL(url), 0);
    }
    fetch("index.json").then(response => response.json()).then(index => {
      state.runs = index.runs || [];
      state.metricCatalog = index.metric_catalog || [];
      state.indexUpdatedAt = index.updated_at || "";
      setText("updated-at", formatDate(state.indexUpdatedAt));
      populateFilters(index);
      render();
    });
    element("case").addEventListener("change", event => { state.caseId = event.target.value; render(); });
    element("metric").addEventListener("change", event => { state.metricKey = event.target.value; render(); });
    element("transport").addEventListener("change", render);
    element("commit").addEventListener("input", render);
    element("date").addEventListener("input", render);
    element("download").addEventListener("click", downloadSelected);
  </script>
</body>
</html>
"""
    (site_dir / "index.html").write_text(document, encoding="utf-8")


def build_site(
    input_dir,
    site_dir,
    commit,
    run_id,
    commit_message="",
    measured_at=None,
    run_url="",
    artifact_url="",
    commit_url="",
):
    input_dir = Path(input_dir)
    site_dir = Path(site_dir)
    metadata = json.loads((input_dir / "metadata.json").read_text(encoding="utf-8"))
    cases = json.loads((input_dir / "cases.json").read_text(encoding="utf-8"))
    results = read_results(input_dir / "results.jsonl")
    measured_at = measured_at or dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    document = run_document(
        metadata,
        cases,
        results,
        commit,
        commit_message,
        run_id,
        measured_at,
        run_url,
        artifact_url,
        commit_url,
    )

    commit_dir = site_dir / "results" / safe_component(commit)
    primary = commit_dir / "arm64.json"
    run_path = commit_dir / "runs" / f"{safe_component(run_id)}.json"
    if not primary.exists():
        write_json(primary, document)
    if not run_path.exists():
        write_json(run_path, document)
    build_index(site_dir)
    write_index_html(site_dir)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--site-dir", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--commit-message", default="")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--measured-at")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--artifact-url", default="")
    parser.add_argument("--commit-url", default="")
    args = parser.parse_args()

    build_site(
        args.input_dir,
        args.site_dir,
        args.commit,
        args.run_id,
        args.commit_message,
        args.measured_at,
        args.run_url,
        args.artifact_url,
        args.commit_url,
    )


if __name__ == "__main__":
    main()
