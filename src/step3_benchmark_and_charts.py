"""
step3_benchmark_and_charts.py
Phụ trách: THÀNH VIÊN 3 (Thực nghiệm Benchmark, Đo đạc & Trực quan hóa)
Nhiệm vụ:
Thực hiện 3 kịch bản thí nghiệm (Benchmark) bằng đo đạc thực tế (time.perf_counter):
1. Thí nghiệm 1 (Volume Scaling): Đo thời gian thực thi thực tế khi tăng số lượng bản ghi
   (100 -> 500 -> 1,000 -> 1,500 -> 10,000 -> 50,000 -> 100,000 -> 200,000 bản ghi).
2. Thí nghiệm 2 (Parallelism / Speedup): Đo thời gian đọc & tổng hợp song song 12 phân vùng
   theo số luồng xử lý (1, 2, 4, 8 workers).
3. Thí nghiệm 3 (Storage & Partition Pruning): Đo thực tế dung lượng lưu trữ và thời gian
   truy vấn lọc theo tháng giữa Bronze CSV (quét toàn bộ) và Silver phân vùng (Partition Pruning).
"""

import csv
import gzip
import json
import time
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
from config import CHARTS_DIR, DEFAULT_RAW_CSV, REPORTS_DIR, SILVER_DIR
from step1_generate_and_profile import generate_raw_dataset
from step2_spark_processing import execute_processing_pipeline, get_dir_size_mb


PRESET_VOLUMES = [100, 500, 1_000, 1_500, 5_000, 10_000, 50_000, 100_000, 200_000]


def run_single_volume_record(rows: int) -> dict:
    """Chạy thật Bước 1 + Bước 2 cho một mốc bản ghi và trả về bản ghi tổng hợp."""
    prof = generate_raw_dataset(num_rows=rows)
    s2 = execute_processing_pipeline()
    log_path = REPORTS_DIR / "pipeline_execution.log"
    logs = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
    total_rev = round(sum(x["total_revenue"] for x in s2.get("gold_kpi_by_category", [])), 2)
    return {
        "rows": rows,
        "clean_rows": s2.get("clean_row_count", 0),
        "dropped_rows": s2.get("removed_invalid_or_dup_rows", 0),
        "clean_pct": s2.get("clean_percentage", 0),
        "gen_time_sec": prof.get("generation_time_sec", 0),
        "read_clean_sec": s2.get("stage_timings", {}).get("read_and_clean_sec", 0),
        "write_sec": s2.get("stage_timings", {}).get("parquet_write_sec", 0),
        "sql_ms": s2.get("stage_timings", {}).get("sql_aggregation_ms", 0),
        "processing_time_sec": s2.get("processing_time_sec", 0),
        "total_pipeline_sec": round(prof.get("generation_time_sec", 0) + s2.get("processing_time_sec", 0), 4),
        "throughput_rows_per_sec": s2.get("throughput_rows_per_sec", 0),
        "latency_us_per_row": s2.get("stage_timings", {}).get("avg_latency_us_per_row", 0),
        "bronze_kb": s2.get("bronze_csv_size_kb", prof.get("file_size_kb", 0)),
        "bronze_mb": s2.get("bronze_csv_size_mb", 0),
        "silver_kb": s2.get("silver_parquet_size_kb", 0),
        "silver_mb": s2.get("silver_parquet_size_mb", 0),
        "storage_saving_pct": s2.get("storage_saving_percent", 0),
        "total_revenue": total_rev,
        "timestamp": s2.get("last_run_timestamp", ""),
        "profiling": prof,
        "summary": s2,
        "logs": logs,
    }


def _measure_partition_worker(part_file):
    rev = 0.0
    cnt = 0
    # Lặp giải nén & tổng hợp để đo tải xử lý phân vùng thực tế
    for _ in range(8):
        with gzip.open(part_file, mode="rt", encoding="utf-8") as gz:
            for line in gz:
                r = json.loads(line)
                if r["status"] == "COMPLETED":
                    rev += float(r["total_amount"])
                    cnt += 1
    return cnt, rev


def _measure_csv_full_scan_month6(csv_path):
    t0 = time.perf_counter()
    rev = 0.0
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["status"] == "COMPLETED" and row["event_time"].startswith("2025-06"):
                try:
                    p = float(row["price"])
                    q = int(row["quantity"])
                    if p > 0 and q > 0:
                        rev += p * q
                except ValueError:
                    pass
    return round((time.perf_counter() - t0) * 1000, 2), rev


def _measure_silver_partition_month6(silver_dir):
    t0 = time.perf_counter()
    rev = 0.0
    part_file = silver_dir / "year=2025" / "month=06" / "part-00000.snappy.parquet.gz"
    if part_file.exists():
        with gzip.open(part_file, mode="rt", encoding="utf-8") as gz:
            for line in gz:
                r = json.loads(line)
                if r["status"] == "COMPLETED":
                    rev += float(r["total_amount"])
    return round((time.perf_counter() - t0) * 1000, 2), rev


def run_benchmarks(volumes=None) -> dict:
    print("\n[BƯỚC 3 - THÀNH VIÊN 3] Đang chạy thực nghiệm đo đạc thực tế & vẽ biểu đồ...")
    if volumes is None:
        volumes = PRESET_VOLUMES

    history_path = REPORTS_DIR / "volume_comparison_history.json"
    history_map = {}
    if history_path.exists():
        try:
            for item in json.loads(history_path.read_text(encoding="utf-8")):
                history_map[int(item["rows"])] = item
        except Exception:
            history_map = {}

    for v in volumes:
        rec = run_single_volume_record(v)
        history_map[int(v)] = rec

    ordered_history = [history_map[k] for k in sorted(history_map.keys())]
    with open(history_path, "w", encoding="utf-8") as hf:
        json.dump(ordered_history, hf, indent=2, ensure_ascii=False)

    # Đảm bảo dữ liệu hiện hành trên đĩa là mốc lớn nhất trong danh sách (thường là 200,000)
    volume_labels = [f"{r['rows']:,}" for r in ordered_history]
    proc_times = [r["processing_time_sec"] for r in ordered_history]
    total_times = [r["total_pipeline_sec"] for r in ordered_history]

    # ==========================================================================
    # THÍ NGHIỆM 2: PARALLELISM TRÊN 12 PHÂN VÙNG SILVER HIỆN TẠI
    # ==========================================================================
    part_files = sorted(SILVER_DIR.rglob("part-*.gz"))
    cores_list = [1, 2, 4, 8]
    parallel_times = []
    for c in cores_list:
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=c) as ex:
            list(ex.map(_measure_partition_worker, part_files))
        parallel_times.append(round(time.perf_counter() - t0, 3))

    # ==========================================================================
    # THÍ NGHIỆM 3: CSV SCAN VS SILVER PARTITION PRUNING (ĐO THỰC TẾ)
    # ==========================================================================
    csv_size_mb = get_dir_size_mb(DEFAULT_RAW_CSV)
    silver_size_mb = get_dir_size_mb(SILVER_DIR)
    csv_query_ms, _ = _measure_csv_full_scan_month6(DEFAULT_RAW_CSV)
    silver_query_ms, _ = _measure_silver_partition_month6(SILVER_DIR)

    benchmark_results = {
        "experiment_1_volume_scaling": {
            "volumes": volume_labels,
            "rows_list": [r["rows"] for r in ordered_history],
            "processing_sec": proc_times,
            "total_pipeline_sec": total_times,
            "single_thread_pandas_sec": total_times,
            "pyspark_distributed_sec": proc_times,
        },
        "experiment_2_parallelism_2M_rows": {
            "cpu_cores": cores_list,
            "processing_time_sec": parallel_times,
        },
        "experiment_3_storage_comparison": {
            "formats": ["Raw CSV (Bronze)", "Partitioned Parquet (Silver)"],
            "size_mb": [csv_size_mb, silver_size_mb],
            "query_time_ms": [csv_query_ms, silver_query_ms],
            "query_time_sec": [round(csv_query_ms / 1000, 4), round(silver_query_ms / 1000, 4)],
        },
    }

    report_path = REPORTS_DIR / "step3_benchmark_metrics.json"
    with open(report_path, "w", encoding="utf-8") as rf:
        json.dump(benchmark_results, rf, indent=4, ensure_ascii=False)

    # VẼ 3 BIỂU ĐỒ PNG TỪ SỐ ĐO THỰC TẾ
    plt.rcParams["font.sans-serif"] = "DejaVu Sans"
    plt.rcParams["axes.edgecolor"] = "#CCCCCC"

    # Biểu đồ 1: Volume Scaling
    fig, ax = plt.subplots(figsize=(8.5, 5), dpi=150)
    ax.plot(volume_labels, total_times, marker="o", linewidth=2.4, color="#E63946", label="Tổng thời gian (Sinh + Xử lý)")
    ax.plot(volume_labels, proc_times, marker="s", linewidth=2.4, color="#1D3557", label="Thời gian xử lý (Đọc + Làm sạch + Ghi Silver)")
    ax.set_title("Thí nghiệm 1: Thời gian thực thi đo thực tế theo số lượng bản ghi", fontsize=11.5, fontweight="bold")
    ax.set_xlabel("Số lượng bản ghi (Rows)", fontsize=10.5)
    ax.set_ylabel("Thời gian (Giây)", fontsize=10.5)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(fontsize=9.5)
    plt.xticks(rotation=20)
    for i, (v1, v2) in enumerate(zip(total_times, proc_times)):
        ax.annotate(f"{v1}s", (volume_labels[i], v1), textcoords="offset points", xytext=(0, 7), ha="center", fontsize=8)
        ax.annotate(f"{v2}s", (volume_labels[i], v2), textcoords="offset points", xytext=(0, -13), ha="center", fontsize=8)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "exp1_volume_scaling.png")
    plt.close()

    # Biểu đồ 2: Parallelism
    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
    core_labels = [f"{c} luồng" for c in cores_list]
    bars = ax.bar(core_labels, parallel_times, color=["#457B9D", "#2A9D8F", "#1D3557", "#F4A261"], width=0.5)
    ax.set_title("Thí nghiệm 2: Thời gian đọc & tổng hợp 12 phân vùng theo số luồng", fontsize=11.5, fontweight="bold")
    ax.set_xlabel("Số luồng xử lý song song (Workers)", fontsize=10.5)
    ax.set_ylabel("Thời gian hoàn thành (Giây)", fontsize=10.5)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f"{yval}s", ha="center", va="bottom", fontweight="bold", fontsize=9)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "exp2_cpu_parallelism.png")
    plt.close()

    # Biểu đồ 3: Storage & Partition Pruning
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.5), dpi=150)
    formats = ["Bronze CSV\n(Quét toàn bộ)", "Silver nén\n(Partition Pruning)"]
    b1 = ax1.bar(formats, [csv_size_mb, silver_size_mb], color=["#E76F51", "#2A9D8F"], width=0.45)
    ax1.set_title("Dung lượng Lưu trữ (MB)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("MB", fontsize=10)
    ax1.grid(axis="y", linestyle="--", alpha=0.5)
    for bar in b1:
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2.0, yval, f"{yval} MB", ha="center", va="bottom", fontweight="bold", fontsize=9)

    b2 = ax2.bar(formats, [csv_query_ms, silver_query_ms], color=["#E76F51", "#1D3557"], width=0.45)
    ax2.set_title("Thời gian truy vấn tháng 06 (ms)", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Mili-giây (ms)", fontsize=10)
    ax2.grid(axis="y", linestyle="--", alpha=0.5)
    for bar in b2:
        yval = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width() / 2.0, yval, f"{yval} ms", ha="center", va="bottom", fontweight="bold", fontsize=9)

    plt.suptitle("Thí nghiệm 3: So sánh lưu trữ & truy vấn thực tế (Bronze CSV vs Silver Partitioned)", fontsize=11.5, fontweight="bold")
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "exp3_storage_comparison.png")
    plt.close()

    print(f"  -> Đã lưu lịch sử so sánh Volume tại: {history_path}")
    print(f"  -> Đã lưu số liệu Benchmark tại: {report_path}")
    return benchmark_results


if __name__ == "__main__":
    run_benchmarks()
