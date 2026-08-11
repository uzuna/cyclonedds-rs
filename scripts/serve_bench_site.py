#!/usr/bin/env python3

import argparse
import datetime as dt
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from bench_site import build_site


class QuietRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format_string, *args):
        return


def default_run_id(input_dir):
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"local-{input_dir.name}-{timestamp}"


def latest_input_dir(results_root):
    candidates = [path.parent for path in results_root.glob("*/metadata.json") if path.is_file()]
    return max(candidates, key=lambda path: (path / "metadata.json").stat().st_mtime, default=None)


def server_url(address):
    host, port = address
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{port}/"


def serve(site_dir, host, port):
    if not (site_dir / "index.html").is_file():
        raise FileNotFoundError(
            f"サイトが見つかりません: {site_dir / 'index.html'}。"
            "ローカル結果がある場合は --input-dir を指定してください。"
        )
    handler = partial(QuietRequestHandler, directory=str(site_dir))
    server = ThreadingHTTPServer((host, port), handler)
    address = server.server_address
    print(f"ベンチマークサイト: {server_url(address)}", flush=True)
    print("終了するには Ctrl-C を押してください。", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("", flush=True)
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(description="ベンチマーク履歴サイトをローカル配信します")
    parser.add_argument("--site-dir", type=Path, default=Path("target/bench-site"))
    parser.add_argument("--input-dir", type=Path, help="指定時はローカル結果からサイトを生成します")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--commit", default="local")
    parser.add_argument("--commit-message", default="ローカルベンチマーク")
    parser.add_argument("--run-id")
    args = parser.parse_args()

    if not args.input_dir and not (args.site_dir / "index.html").is_file():
        args.input_dir = latest_input_dir(Path("target/bench-results"))
        if args.input_dir:
            print(f"最新のローカル結果からサイトを生成します: {args.input_dir}", flush=True)

    if args.input_dir:
        if not args.run_id:
            args.run_id = default_run_id(args.input_dir)
        build_site(
            args.input_dir,
            args.site_dir,
            args.commit,
            args.run_id,
            args.commit_message,
        )
        print(f"サイトを生成しました: {args.site_dir}", flush=True)

    serve(args.site_dir, args.host, args.port)


if __name__ == "__main__":
    main()
