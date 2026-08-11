import importlib.util
import json
import re
import signal
import subprocess
import sys
import tempfile
import urllib.request
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("bench_site.py")
SPEC = importlib.util.spec_from_file_location("bench_site", MODULE_PATH)
BENCH_SITE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH_SITE)
SERVE_PATH = MODULE_PATH.with_name("serve_bench_site.py")


class BenchSiteTest(unittest.TestCase):
    def test_index_contains_metric_catalog_and_commit_links(self):
        with tempfile.TemporaryDirectory() as directory:
            input_dir = Path(directory) / "input"
            site_dir = Path(directory) / "site"
            input_dir.mkdir()
            (input_dir / "metadata.json").write_text(
                json.dumps({"vendor": {"cyclonedds_sha": {"value": "vendor-a"}}}),
                encoding="utf-8",
            )
            (input_dir / "cases.json").write_text(
                json.dumps([{"id": "latency-256b-udp"}]),
                encoding="utf-8",
            )
            result = {
                "case_id": "latency-256b-udp",
                "transport": "udp",
                "status": "ok",
                "throughput_bytes_per_sec": None,
                "latency": {"p50_ns": 100, "p95_ns": 150, "p99_ns": 175},
                "serialize_elapsed_ns": None,
                "deserialize_elapsed_ns": None,
            }
            (input_dir / "results.jsonl").write_text(json.dumps(result) + "\n", encoding="utf-8")
            BENCH_SITE.build_site(
                input_dir,
                site_dir,
                "commit-a",
                "run-a",
                "Improve benchmark rendering",
                "2026-08-11T00:00:00Z",
                "https://example.test/workflow",
                "https://example.test/artifact",
                "https://example.test/commit",
            )

            index = json.loads((site_dir / "index.json").read_text(encoding="utf-8"))
            self.assertEqual(index["metric_catalog"], list(BENCH_SITE.METRIC_DEFINITIONS))
            self.assertEqual(index["runs"][0]["commit_url"], "https://example.test/commit")
            self.assertEqual(index["runs"][0]["commit_message"], "Improve benchmark rendering")
            self.assertEqual(
                index["runs"][0]["metrics"][0],
                {
                    "case_id": "latency-256b-udp",
                    "transport": "udp",
                    "status": "ok",
                    "throughput_bytes_per_sec": None,
                    "latency_p50_ns": 100,
                    "latency_p95_ns": 150,
                    "latency_p99_ns": 175,
                    "serialize_elapsed_ns": None,
                    "deserialize_elapsed_ns": None,
                },
            )
            html = (site_dir / "index.html").read_text(encoding="utf-8")
            for required in ("id=\"chart\"", "id=\"metric\"", "id=\"download\"", "createSvg", "メッセージ"):
                self.assertIn(required, html)
            script = re.search(r"<script>\n(.*?)\n  </script>", html, re.DOTALL)
            self.assertIsNotNone(script)
            checked = subprocess.run(
                ["node", "--check"],
                input=script.group(1),
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_local_server_serves_generated_site(self):
        with tempfile.TemporaryDirectory() as directory:
            site_dir = Path(directory) / "site"
            site_dir.mkdir()
            (site_dir / "index.html").write_text("<h1>local benchmark</h1>\n", encoding="utf-8")
            process = subprocess.Popen(
                [sys.executable, str(SERVE_PATH), "--site-dir", str(site_dir), "--port", "0"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                address_line = process.stdout.readline().strip()
                if not address_line:
                    error = process.stderr.read()
                    if "Operation not permitted" in error:
                        self.skipTest("この実行環境では localhost の bind が許可されていません")
                    self.fail(error or "ローカルサーバーが起動しませんでした")
                address = address_line.split(": ", 1)[1]
                with urllib.request.urlopen(address, timeout=2) as response:
                    self.assertEqual(response.read().decode("utf-8"), "<h1>local benchmark</h1>\n")
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    process.wait(timeout=2)
                process.stdout.close()
                process.stderr.close()


if __name__ == "__main__":
    unittest.main()
