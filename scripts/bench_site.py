#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import re
from pathlib import Path


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


def run_document(metadata, cases, results, commit, run_id, measured_at, run_url, artifact_url):
    return {
        "schema_version": 1,
        "run_id": run_id,
        "commit": commit,
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
                "status": result.get("status"),
                "throughput_bytes_per_sec": result.get("throughput_bytes_per_sec"),
                "latency_p50_ns": latency.get("p50_ns"),
                "latency_p95_ns": latency.get("p95_ns"),
                "serialize_elapsed_ns": result.get("serialize_elapsed_ns"),
                "deserialize_elapsed_ns": result.get("deserialize_elapsed_ns"),
            }
        )
    return {
        "run_id": document.get("run_id"),
        "commit": document.get("commit"),
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
  <title>CycloneDDS Rust benchmark history</title>
  <style>
    :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
    body { margin: 2rem auto; max-width: 1200px; padding: 0 1rem; }
    form { display: grid; gap: .75rem; grid-template-columns: repeat(auto-fit, minmax(12rem, 1fr)); margin: 1rem 0; }
    label { display: grid; gap: .25rem; }
    input, select { font: inherit; padding: .35rem; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #8886; padding: .5rem; text-align: left; vertical-align: top; }
    code { overflow-wrap: anywhere; }
    .ok { color: #187b35; }
    .error { color: #b3261e; }
  </style>
</head>
<body>
  <h1>CycloneDDS Rust benchmark history</h1>
  <form id="filters">
    <label>Commit<input id="commit" type="search" placeholder="commit prefix"></label>
    <label>Date<input id="date" type="date"></label>
    <label>Transport<select id="transport"><option value="">all</option></select></label>
    <label>Case<input id="case" type="search" placeholder="case id"></label>
  </form>
  <table>
    <thead><tr><th>Date</th><th>Commit</th><th>Cyclone DDS</th><th>Transport</th><th>Cases</th><th>Metrics</th><th>Status</th><th>Links</th></tr></thead>
    <tbody id="runs"></tbody>
  </table>
  <script>
    const state = { runs: [] };
    const ids = ["commit", "date", "transport", "case"];
    const element = id => document.getElementById(id);
    const link = (href, label) => {
      if (!href) return "";
      const anchor = document.createElement("a");
      anchor.href = href;
      anchor.textContent = label;
      return anchor;
    };
    function matches(run) {
      const commit = element("commit").value.trim().toLowerCase();
      const date = element("date").value;
      const transport = element("transport").value;
      const caseId = element("case").value.trim().toLowerCase();
      return (!commit || (run.commit || "").toLowerCase().startsWith(commit))
        && (!date || (run.measured_at || "").startsWith(date))
        && (!transport || run.transports.includes(transport))
        && (!caseId || run.cases.some(value => value.toLowerCase().includes(caseId)));
    }
    function render() {
      const tbody = element("runs");
      tbody.replaceChildren();
      for (const run of state.runs.filter(matches)) {
        const row = document.createElement("tr");
        const values = [run.measured_at || "", run.commit || "", run.cyclonedds_sha || "", run.transports.join(", "), run.cases.join(", ")];
        for (const value of values) {
          const cell = document.createElement("td");
          cell.textContent = value;
          row.append(cell);
        }
        const metrics = document.createElement("td");
        metrics.textContent = (run.metrics || []).map(metric => {
          const values = [];
          if (metric.throughput_bytes_per_sec !== null && metric.throughput_bytes_per_sec !== undefined) values.push(`throughput=${metric.throughput_bytes_per_sec} B/s`);
          if (metric.latency_p50_ns !== null && metric.latency_p50_ns !== undefined) values.push(`p50=${metric.latency_p50_ns} ns`);
          if (metric.serialize_elapsed_ns !== null && metric.serialize_elapsed_ns !== undefined) values.push(`serialize=${metric.serialize_elapsed_ns} ns`);
          if (metric.deserialize_elapsed_ns !== null && metric.deserialize_elapsed_ns !== undefined) values.push(`deserialize=${metric.deserialize_elapsed_ns} ns`);
          return `${metric.case_id}: ${values.join(", ") || metric.status}`;
        }).join("; ");
        row.append(metrics);
        const status = document.createElement("td");
        status.textContent = (run.statuses || []).join(", ");
        row.append(status);
        const links = document.createElement("td");
        const result = link(run.result_path, "results");
        if (result) links.append(result);
        if (run.run_url) { links.append(" "); links.append(link(run.run_url, "workflow")); }
        if (run.artifact_url) { links.append(" "); links.append(link(run.artifact_url, "artifact")); }
        row.append(links);
        tbody.append(row);
      }
    }
    fetch("index.json").then(response => response.json()).then(index => {
      state.runs = index.runs || [];
      const transports = [...new Set(state.runs.flatMap(run => run.transports || []))].sort();
      for (const value of transports) {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        element("transport").append(option);
      }
      render();
    });
    ids.forEach(id => element(id).addEventListener("input", render));
  </script>
</body>
</html>
"""
    (site_dir / "index.html").write_text(document, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--site-dir", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--measured-at")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--artifact-url", default="")
    args = parser.parse_args()

    input_dir = args.input_dir
    site_dir = args.site_dir
    metadata = json.loads((input_dir / "metadata.json").read_text(encoding="utf-8"))
    cases = json.loads((input_dir / "cases.json").read_text(encoding="utf-8"))
    results = read_results(input_dir / "results.jsonl")
    measured_at = args.measured_at or dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    document = run_document(
        metadata,
        cases,
        results,
        args.commit,
        args.run_id,
        measured_at,
        args.run_url,
        args.artifact_url,
    )

    commit_dir = site_dir / "results" / safe_component(args.commit)
    primary = commit_dir / "arm64.json"
    run_path = commit_dir / "runs" / f"{safe_component(args.run_id)}.json"
    if not primary.exists():
        write_json(primary, document)
    if not run_path.exists():
        write_json(run_path, document)
    build_index(site_dir)
    write_index_html(site_dir)


if __name__ == "__main__":
    main()
