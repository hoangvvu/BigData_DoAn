"""
web_dashboard.py
Web hiển thị kết quả đồ án (quy trình Storage -> Processing), mục 5.1 - 5.8,
kèm chức năng thay đổi số bản ghi (Volume: 100, 500, 1000, 1500, ...) để chạy và so sánh trực tiếp.
Truy cập tại: http://localhost:8080
"""

import json
import platform
import sys
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC_DIR))

from config import CHARTS_DIR, GOLD_DIR, REPORTS_DIR, SCHEMA_CONTRACT  # noqa: E402
from step3_benchmark_and_charts import run_single_volume_record  # noqa: E402

PORT = 8080
UI_FILE = SRC_DIR / "dashboard_ui.html"
HISTORY_FILE = REPORTS_DIR / "volume_comparison_history.json"
_run_lock = threading.Lock()


def read_json(path: Path):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_history() -> list:
    data = read_json(HISTORY_FILE)
    if isinstance(data, list):
        return sorted(data, key=lambda x: int(x.get("rows", 0)))
    return []


def save_to_history(record: dict) -> list:
    items = {int(x["rows"]): x for x in load_history() if "rows" in x}
    items[int(record["rows"])] = record
    ordered = [items[k] for k in sorted(items.keys())]
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(ordered, f, indent=2, ensure_ascii=False)
    return ordered


def build_report() -> dict:
    log_path = REPORTS_DIR / "pipeline_execution.log"
    gold = read_json(GOLD_DIR / "kpi_revenue_by_category.json")
    return {
        "summary": read_json(REPORTS_DIR / "step2_processing_summary.json"),
        "profiling": read_json(REPORTS_DIR / "step1_bronze_profiling.json"),
        "benchmark": read_json(REPORTS_DIR / "step3_benchmark_metrics.json"),
        "history": load_history(),
        "gold_sample": gold[:2] if isinstance(gold, list) else [],
        "schema": SCHEMA_CONTRACT,
        "logs": log_path.read_text(encoding="utf-8") if log_path.exists() else "",
        "charts": sorted(p.name for p in CHARTS_DIR.glob("*.png")),
        "env": {
            "python": platform.python_version(),
            "os": platform.system() + " " + platform.release(),
        },
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def _send(self, code, ctype, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/api/report":
            body = json.dumps(build_report(), ensure_ascii=False).encode("utf-8")
            return self._send(200, "application/json; charset=utf-8", body)
        if path.startswith("/charts/"):
            f = CHARTS_DIR / Path(urllib.parse.unquote(path)).name
            if f.is_file() and f.suffix == ".png":
                return self._send(200, "image/png", f.read_bytes())
            return self.send_error(404)
        if path in ("/", "/index.html"):
            return self._send(200, "text/html; charset=utf-8", UI_FILE.read_bytes())
        self.send_error(404)

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/api/run-volume":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
                payload = json.loads(raw_body or "{}")
                with _run_lock:
                    volumes = payload.get("volumes")
                    if isinstance(volumes, list) and volumes:
                        for v in volumes:
                            rows = max(10, min(int(v), 2_000_000))
                            rec = run_single_volume_record(rows)
                            save_to_history(rec)
                    else:
                        rows = max(10, min(int(payload.get("rows", 1000)), 2_000_000))
                        rec = run_single_volume_record(rows)
                        save_to_history(rec)
                body = json.dumps(build_report(), ensure_ascii=False).encode("utf-8")
                return self._send(200, "application/json; charset=utf-8", body)
            except Exception as exc:  # noqa: BLE001
                err = json.dumps({"error": str(exc)}, ensure_ascii=False).encode("utf-8")
                return self._send(500, "application/json; charset=utf-8", err)
        self.send_error(404)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print("-" * 60)
    print(f" Web đồ án Big Data: http://localhost:{PORT}")
    print(" Nhấn Ctrl+C để dừng.")
    print("-" * 60)
    webbrowser.open(f"http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
