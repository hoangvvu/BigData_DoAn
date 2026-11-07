"""
run_pipeline.py - Script chạy tự động toàn bộ Pipeline Đồ án Big Data từ Bước 1 -> Bước 3.
Cách chạy:
    py run_pipeline.py --rows 200000
"""

import argparse
import sys
from pathlib import Path

# Thêm thư mục src vào sys.path
SRC_DIR = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(SRC_DIR))

from step1_generate_and_profile import generate_raw_dataset
from step2_spark_processing import execute_processing_pipeline
from step3_benchmark_and_charts import run_benchmarks


def main():
    parser = argparse.ArgumentParser(
        description="Chạy trọn bộ Pipeline Big Data: Storage (Bronze) -> Processing (Silver/Gold) -> Benchmark"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=200_000,
        help="Số lượng bản ghi giao dịch cần sinh để chạy pipeline (mặc định: 200,000)",
    )
    args = parser.parse_args()

    print("=" * 75)
    print("   ĐỒ ÁN NHẬP MÔN DỮ LIỆU LỚN: QUY TRÌNH STORAGE -> PROCESSING")
    print("=" * 75)

    # Bước 1: Thành viên 1 - Sinh dữ liệu thô (Bronze) & Kiểm tra chất lượng đầu vào
    generate_raw_dataset(num_rows=args.rows)

    # Bước 2: Thành viên 2 - Làm sạch, chuẩn hóa với PySpark & Lưu Parquet (Silver/Gold)
    execute_processing_pipeline()

    # Bước 3: Thành viên 3 - Chạy thực nghiệm Benchmark & Vẽ 3 biểu đồ báo cáo
    run_benchmarks()

    print("\n" + "=" * 75)
    print("   HOÀN TẤT TOÀN BỘ PIPELINE! KIỂM TRA KẾT QUẢ TẠI:")
    print("   - Dữ liệu Bronze/Silver/Gold : ./data/")
    print("   - Báo cáo số liệu JSON       : ./output_reports/")
    print("   - Biểu đồ thực nghiệm PNG    : ./output_charts/")
    print("=" * 75)


if __name__ == "__main__":
    main()

