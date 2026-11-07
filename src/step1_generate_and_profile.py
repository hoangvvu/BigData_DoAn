"""
step1_generate_and_profile.py
Phụ trách: THÀNH VIÊN 1 (Data & Storage - Tầng Bronze)
Nhiệm vụ:
1. Sinh dữ liệu giao dịch Thương mại điện tử quy mô lớn (Raw CSV) đóng vai trò là dữ liệu
   đầu vào trong Storage (Tầng Bronze).
2. Cố tình cài cắm ~12% dữ liệu lỗi thực tế (Veracity):
   - Bản ghi trùng lặp (Duplicate transaction_id)
   - Giá trị khuyết thiếu (NULL ở user_id, price)
   - Giá trị ngoại lai/sai quy tắc nghiệp vụ (price <= 0, quantity <= 0)
   - Sai định dạng thời gian (INVALID_DATE)
3. Thực hiện Data Profiling (Kiểm định chất lượng dữ liệu ban đầu) và lưu báo cáo JSON.
"""

import argparse
import csv
import json
import os
import random
import time
from datetime import datetime, timedelta
from config import (
    BRONZE_DIR,
    CATEGORIES,
    CITIES,
    DEFAULT_RAW_CSV,
    PAYMENT_METHODS,
    REPORTS_DIR,
    STATUSES,
)


def generate_raw_dataset(num_rows: int = 200_000, output_path=DEFAULT_RAW_CSV) -> dict:
    """Sinh dữ liệu giao dịch thô vào tầng Bronze và thống kê lỗi gài vào."""
    print(f"\n[BƯỚC 1 - THÀNH VIÊN 1] Đang sinh {num_rows:,} bản ghi vào tầng Bronze...")
    start_time = time.perf_counter()
    random.seed(42)

    base_time = datetime(2025, 1, 1, 0, 0, 0)
    stats = {
        "total_generated": 0,
        "clean_rows_injected": 0,
        "null_injected": 0,
        "negative_or_zero_injected": 0,
        "invalid_date_injected": 0,
        "duplicate_injected": 0,
    }

    headers = [
        "transaction_id",
        "user_id",
        "product_id",
        "category",
        "price",
        "quantity",
        "event_time",
        "payment_method",
        "city",
        "status",
    ]

    sample_for_duplicates = []

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(1, num_rows + 1):
            r = random.random()

            # 1. Khoảng 4% bản ghi trùng lặp hoàn toàn với bản ghi trước đó
            if r < 0.04 and sample_for_duplicates:
                dup_row = random.choice(sample_for_duplicates)
                writer.writerow(dup_row)
                stats["duplicate_injected"] += 1
                stats["total_generated"] += 1
                continue

            txn_id = f"TXN{i:08d}"
            user_id = f"USR{random.randint(1, 50000):06d}"
            product_id = f"PRD{random.randint(1, 5000):05d}"
            category = random.choice(CATEGORIES)
            price = round(random.uniform(5.0, 1500.0), 2)
            quantity = random.randint(1, 10)
            random_seconds = random.randint(0, 365 * 24 * 3600)
            event_time = (base_time + timedelta(seconds=random_seconds)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            payment = random.choice(PAYMENT_METHODS)
            city = random.choice(CITIES)
            status = random.choice(STATUSES)

            # 2. Khoảng 4% bản ghi bị khuyết thiếu (NULL) ở user_id hoặc price
            if 0.04 <= r < 0.08:
                if random.random() < 0.5:
                    user_id = ""
                else:
                    price = ""
                stats["null_injected"] += 1

            # 3. Khoảng 2.5% bản ghi có giá tiền âm hoặc số lượng <= 0 (Outlier/Invalid)
            elif 0.08 <= r < 0.105:
                if random.random() < 0.5:
                    price = round(random.uniform(-500.0, -1.0), 2)
                else:
                    quantity = random.choice([0, -1, -5])
                stats["negative_or_zero_injected"] += 1

            # 4. Khoảng 1.5% bản ghi bị lỗi định dạng thời gian
            elif 0.105 <= r < 0.12:
                event_time = "2025-99-99 INVALID_TIME"
                stats["invalid_date_injected"] += 1
            else:
                stats["clean_rows_injected"] += 1

            row = [
                txn_id,
                user_id,
                product_id,
                category,
                price,
                quantity,
                event_time,
                payment,
                city,
                status,
            ]
            writer.writerow(row)
            stats["total_generated"] += 1

            if len(sample_for_duplicates) < 500 and r >= 0.12:
                sample_for_duplicates.append(row)

    raw_elapsed = time.perf_counter() - start_time
    elapsed = round(raw_elapsed, 4 if raw_elapsed < 1.0 else 2)
    file_size_bytes = os.path.getsize(output_path)
    file_size_kb = round(file_size_bytes / 1024, 2)
    mb_val = file_size_bytes / (1024 * 1024)
    file_size_mb = round(mb_val, 4 if mb_val < 1.0 else 2)

    stats["file_path"] = str(output_path)
    stats["file_size_bytes"] = file_size_bytes
    stats["file_size_kb"] = file_size_kb
    stats["file_size_mb"] = file_size_mb
    stats["generation_time_sec"] = elapsed
    stats["throughput_rows_per_sec"] = int(num_rows / max(raw_elapsed, 0.0001))

    report_path = REPORTS_DIR / "step1_bronze_profiling.json"
    with open(report_path, "w", encoding="utf-8") as rf:
        json.dump(stats, rf, indent=4, ensure_ascii=False)

    print(f"  -> Hoàn tất sinh dữ liệu trong {elapsed}s ({file_size_mb} MB)")
    print(f"  -> Tổng số bản ghi: {stats['total_generated']:,}")
    print(f"  -> Bản ghi hợp lệ: {stats['clean_rows_injected']:,}")
    print(
        f"  -> Bản ghi lỗi gài vào: "
        f"Trùng lặp={stats['duplicate_injected']:,}, "
        f"Null={stats['null_injected']:,}, "
        f"Giá/SL âm={stats['negative_or_zero_injected']:,}, "
        f"Sai ngày={stats['invalid_date_injected']:,}"
    )
    print(f"  -> Đã lưu báo cáo Profiling tại: {report_path}")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sinh dữ liệu tầng Bronze (Raw CSV)")
    parser.add_argument(
        "--rows",
        type=int,
        default=200_000,
        help="Số lượng bản ghi cần sinh (mặc định: 200,000)",
    )
    args = parser.parse_args()
    generate_raw_dataset(num_rows=args.rows)

