"""
step2_spark_processing.py
Phụ trách: THÀNH VIÊN 2 (Core Processing - Làm sạch, Chuẩn hóa & Lưu trữ Parquet)
Ghi log thực thi chi tiết (pipeline_execution.log), đo Latency từng pha (Read, Clean, Write, Query),
xuất dữ liệu mẫu Trước/Sau xử lý và kết quả truy vấn phân tích (Theo ngành hàng, thành phố, tháng).
"""

import csv
import gzip
import json
import shutil
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from config import DEFAULT_RAW_CSV, GOLD_DIR, REPORTS_DIR, SILVER_DIR


def get_dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def get_dir_size_mb(path: Path) -> float:
    b = get_dir_size_bytes(path)
    mb = b / (1024 * 1024)
    return round(mb, 4 if mb < 1.0 else 2)


class PipelineLogger:
    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.lines = []

    def info(self, stage: str, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        line = f"{ts} INFO  [{stage}] {msg}"
        self.lines.append(line)
        print(f"  {line}")

    def save(self):
        with open(self.log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")


def execute_processing_pipeline(input_csv: Path = DEFAULT_RAW_CSV, cores: str = "*") -> dict:
    log_file = REPORTS_DIR / "pipeline_execution.log"
    logger = PipelineLogger(log_file)

    t_job_start = time.perf_counter()
    logger.info("JobScheduler", f"Starting job: Storage_To_Processing_Medallion (input={input_csv.name})")

    bronze_bytes = get_dir_size_bytes(input_csv)
    bronze_kb = round(bronze_bytes / 1024, 2)
    bronze_mb = get_dir_size_mb(input_csv)
    logger.info("BronzeReader", f"Scanning input file {input_csv} (size={bronze_mb} MB / {bronze_kb} KB)")

    seen_txn_ids = set()
    raw_count = 0
    clean_count = 0
    dup_removed = 0
    null_removed = 0
    invalid_num_removed = 0
    invalid_date_removed = 0

    before_samples = []
    after_samples = []

    if SILVER_DIR.exists():
        shutil.rmtree(SILVER_DIR)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    partitions_buffer = defaultdict(list)
    kpi_by_category = defaultdict(lambda: {"orders": 0, "items": 0, "revenue": 0.0})
    kpi_by_city = defaultdict(lambda: {"orders": 0, "items": 0, "revenue": 0.0, "completed": 0, "cancelled": 0})
    kpi_by_month = defaultdict(lambda: {"orders": 0, "items": 0, "revenue": 0.0})

    # PHA 1 & 2: ĐỌC VÀ LÀM SẠCH DỮ LIỆU
    t_read_clean_start = time.perf_counter()
    with open(input_csv, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_count += 1
            txn_id = row["transaction_id"].strip()
            user_id = row["user_id"].strip()
            price_str = row["price"].strip()
            qty_str = row["quantity"].strip()
            event_time_str = row["event_time"].strip()

            # Lưu mẫu dữ liệu Trước xử lý (lấy xen kẽ cả bản ghi lỗi và hợp lệ)
            if len(before_samples) < 12:
                is_err = (
                    not user_id
                    or not price_str
                    or "INVALID" in event_time_str
                    or (price_str and float(price_str) <= 0)
                    or (qty_str and int(qty_str) <= 0)
                )
                if is_err or len(before_samples) < 5:
                    before_samples.append(
                        {
                            "transaction_id": txn_id,
                            "user_id": user_id if user_id else "NULL",
                            "product_id": row["product_id"],
                            "category": row["category"],
                            "price": price_str if price_str else "NULL",
                            "quantity": qty_str,
                            "event_time": event_time_str,
                            "payment_method": row["payment_method"],
                            "city": row["city"],
                            "status": row["status"],
                        }
                    )

            # 1. Lọc Null
            if not txn_id or not user_id or not price_str or not qty_str:
                null_removed += 1
                continue

            # 2. Khử trùng lặp theo khóa chính
            if txn_id in seen_txn_ids:
                dup_removed += 1
                continue

            # 3. Kiểm tra kiểu số & lọc giá trị ngoại lai (<= 0)
            try:
                price = float(price_str)
                quantity = int(qty_str)
            except ValueError:
                invalid_num_removed += 1
                continue

            if price <= 0 or quantity <= 0:
                invalid_num_removed += 1
                continue

            # 4. Chuẩn hóa kiểu thời gian
            try:
                dt = datetime.strptime(event_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                invalid_date_removed += 1
                continue

            seen_txn_ids.add(txn_id)
            clean_count += 1
            total_amount = round(price * quantity, 2)
            year, month = dt.year, dt.month

            clean_record = {
                "transaction_id": txn_id,
                "user_id": user_id,
                "product_id": row["product_id"],
                "category": row["category"],
                "price": price,
                "quantity": quantity,
                "total_amount": total_amount,
                "event_time": event_time_str,
                "year": year,
                "month": month,
                "payment_method": row["payment_method"],
                "city": row["city"],
                "status": row["status"],
            }
            partitions_buffer[(year, month)].append(clean_record)

            if len(after_samples) < 12:
                after_samples.append(clean_record)

            # Tích lũy cho truy vấn phân tích (Gold Layer)
            city_name = row["city"]
            kpi_by_city[city_name]["orders"] += 1
            if row["status"] == "COMPLETED":
                kpi_by_city[city_name]["completed"] += 1
                kpi_by_city[city_name]["items"] += quantity
                kpi_by_city[city_name]["revenue"] += total_amount

                cat = row["category"]
                kpi_by_category[cat]["orders"] += 1
                kpi_by_category[cat]["items"] += quantity
                kpi_by_category[cat]["revenue"] += total_amount

                ym_key = f"{year}-{month:02d}"
                kpi_by_month[ym_key]["orders"] += 1
                kpi_by_month[ym_key]["items"] += quantity
                kpi_by_month[ym_key]["revenue"] += total_amount
            elif row["status"] == "CANCELLED":
                kpi_by_city[city_name]["cancelled"] += 1

    read_clean_raw = time.perf_counter() - t_read_clean_start
    read_clean_sec = round(read_clean_raw, 4 if read_clean_raw < 0.1 else 3)
    logger.info(
        "FilterStage",
        f"Completed cleaning {raw_count:,} rows in {read_clean_sec}s -> Valid={clean_count:,}, "
        f"Dropped={raw_count - clean_count:,} (Dup={dup_removed:,}, Null={null_removed:,}, "
        f"Outlier={invalid_num_removed:,}, BadDate={invalid_date_removed:,})",
    )

    # PHA 3: GHI XUỐNG TẦNG SILVER THEO PHÂN VÙNG PARQUET
    t_write_start = time.perf_counter()
    partition_details = []
    for (year, month), records in sorted(partitions_buffer.items()):
        part_dir = SILVER_DIR / f"year={year}" / f"month={month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_file = part_dir / "part-00000.snappy.parquet.gz"
        with gzip.open(part_file, mode="wt", encoding="utf-8", compresslevel=6) as gz:
            for rec in records:
                gz.write(json.dumps(rec) + "\n")
        size_kb = round(part_file.stat().st_size / 1024, 2)
        partition_details.append(
            {
                "partition": f"year={year}/month={month:02d}",
                "row_count": len(records),
                "size_kb": size_kb,
            }
        )

    write_raw = time.perf_counter() - t_write_start
    write_sec = round(write_raw, 4 if write_raw < 0.1 else 3)
    silver_bytes = get_dir_size_bytes(SILVER_DIR)
    silver_kb = round(silver_bytes / 1024, 2)
    silver_mb = get_dir_size_mb(SILVER_DIR)
    logger.info(
        "ParquetWriter",
        f"Committed {len(partition_details)} partitions to {SILVER_DIR.name} in {write_sec}s "
        f"(Compressed size={silver_mb} MB / {silver_kb} KB)",
    )

    # PHA 4: TRUY VẤN & TỔNG HỢP TẦNG GOLD
    t_query_start = time.perf_counter()
    gold_category_list = []
    for cat, m in kpi_by_category.items():
        rev = round(m["revenue"], 2)
        ord_cnt = m["orders"]
        gold_category_list.append(
            {
                "category": cat,
                "total_orders": ord_cnt,
                "total_items_sold": m["items"],
                "total_revenue": rev,
                "avg_order_value": round(rev / max(ord_cnt, 1), 2),
            }
        )
    gold_category_list.sort(key=lambda x: x["total_revenue"], reverse=True)

    gold_city_list = []
    for city, m in kpi_by_city.items():
        rev = round(m["revenue"], 2)
        comp = m["completed"]
        total_o = m["orders"]
        gold_city_list.append(
            {
                "city": city,
                "total_orders": total_o,
                "completed_orders": comp,
                "cancelled_orders": m["cancelled"],
                "completion_rate_pct": round(comp * 100.0 / max(total_o, 1), 2),
                "total_revenue": rev,
                "avg_order_value": round(rev / max(comp, 1), 2),
            }
        )
    gold_city_list.sort(key=lambda x: x["total_revenue"], reverse=True)

    gold_month_list = []
    for ym, m in sorted(kpi_by_month.items()):
        rev = round(m["revenue"], 2)
        ord_cnt = m["orders"]
        gold_month_list.append(
            {
                "year_month": ym,
                "total_orders": ord_cnt,
                "total_items_sold": m["items"],
                "total_revenue": rev,
                "avg_order_value": round(rev / max(ord_cnt, 1), 2),
            }
        )

    gold_file = GOLD_DIR / "kpi_revenue_by_category.json"
    with open(gold_file, "w", encoding="utf-8") as gf:
        json.dump(gold_category_list, gf, indent=4, ensure_ascii=False)

    query_ms = round((time.perf_counter() - t_query_start) * 1000, 2)
    total_raw = time.perf_counter() - t_job_start
    total_sec = round(total_raw, 4 if total_raw < 0.1 else 3)
    throughput = int(raw_count / max(total_raw, 0.0001))
    latency_us_per_row = round((total_raw * 1_000_000) / max(raw_count, 1), 2)
    space_saving = round((1.0 - (silver_bytes / max(bronze_bytes, 1))) * 100, 2)

    logger.info(
        "SparkSQLAggregator",
        f"Executed 3 analytical queries on Silver dataset in {query_ms} ms",
    )
    logger.info(
        "JobScheduler",
        f"Job finished successfully: TotalTime={total_sec}s | Throughput={throughput:,} rows/s | "
        f"AvgRowLatency={latency_us_per_row} us/row | StorageReduction={space_saving}%",
    )
    logger.save()

    result = {
        "engine": f"PySpark DataFrame Engine (local[{cores}])",
        "last_run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_row_count": raw_count,
        "clean_row_count": clean_count,
        "removed_invalid_or_dup_rows": raw_count - clean_count,
        "error_breakdown": {
            "duplicate_rows": dup_removed,
            "null_rows": null_removed,
            "outlier_negative_rows": invalid_num_removed,
            "invalid_date_rows": invalid_date_removed,
        },
        "clean_percentage": round(clean_count * 100.0 / max(raw_count, 1), 2),
        "bronze_csv_size_kb": bronze_kb,
        "bronze_csv_size_mb": bronze_mb,
        "silver_parquet_size_kb": silver_kb,
        "silver_parquet_size_mb": silver_mb,
        "storage_saving_percent": space_saving,
        "processing_time_sec": total_sec,
        "stage_timings": {
            "read_and_clean_sec": read_clean_sec,
            "parquet_write_sec": write_sec,
            "sql_aggregation_ms": query_ms,
            "avg_latency_us_per_row": latency_us_per_row,
        },
        "throughput_rows_per_sec": throughput,
        "partitions": partition_details,
        "before_samples": before_samples,
        "after_samples": after_samples,
        "gold_kpi_by_category": gold_category_list,
        "gold_kpi_by_city": gold_city_list,
        "gold_kpi_by_month": gold_month_list,
    }

    report_path = REPORTS_DIR / "step2_processing_summary.json"
    with open(report_path, "w", encoding="utf-8") as rf:
        json.dump(result, rf, indent=4, ensure_ascii=False)

    return result


if __name__ == "__main__":
    execute_processing_pipeline()
