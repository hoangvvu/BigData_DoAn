# Đồ Án Học Phần: Nhập Môn Dữ Liệu Lớn (Big Data)

## 1. Thông tin đề tài
- **Quy trình lựa chọn:** `Storage → Processing` (Làm sạch, chuẩn hóa, kết hợp và tối ưu hóa lưu trữ dữ liệu lớn theo kiến trúc Data Lake Medallion: **Bronze $\rightarrow$ Silver $\rightarrow$ Gold**).
- **Bài toán thực tế:** Xử lý và chuẩn hóa dữ liệu Giao dịch Thương mại điện tử quy mô lớn (hàng triệu bản ghi) chứa dữ liệu bẩn (trùng lặp, khuyết thiếu `NULL`, giá trị âm ngoại lai, sai định dạng thời gian).
- **Công nghệ sử dụng:** Python, Apache Spark (`PySpark`), Spark SQL, Định dạng lưu trữ cột nén `Parquet` có phân vùng (`Partitioning`), `Matplotlib`.

---

## 2. Cấu trúc thư mục dự án

```text
DoAn/
├── data/
│   ├── bronze_raw/                  # [Input] Dữ liệu CSV thô ban đầu (chứa ~12% lỗi)
│   ├── silver_cleaned/              # [Output 1] Dữ liệu sạch Parquet phân vùng theo year/month
│   └── gold_aggregated/             # [Output 2] Bảng tổng hợp KPI doanh thu theo ngành hàng
├── src/
│   ├── config.py                    # Cấu hình đường dẫn & Hợp đồng dữ liệu (Data Contract)
│   ├── step1_generate_and_profile.py# (Thành viên 1) Sinh dữ liệu lớn & Kiểm định đầu vào
│   ├── step2_spark_processing.py    # (Thành viên 2) Job PySpark làm sạch, chuẩn hóa & xuất Parquet
│   ├── step3_benchmark_and_charts.py# (Thành viên 3) Thực nghiệm Benchmark & Vẽ 3 biểu đồ PNG
│   ├── web_dashboard.py             # Web Server hiển thị mục 5.1-5.8 & chạy so sánh đa mốc Volume
│   └── dashboard_ui.html            # Giao diện Web Dashboard
├── output_reports/                  # Chứa các file JSON thống kê số liệu trước/sau xử lý & lịch sử Volume
├── output_charts/                   # Chứa 3 biểu đồ thực nghiệm độ phân giải cao cho báo cáo
├── run_pipeline.py                  # Script chạy tự động toàn bộ pipeline chỉ với 1 lệnh
├── start_dashboard.bat              # Khởi động nhanh Web Dashboard tại http://localhost:8080
├── requirements.txt                 # Danh sách thư viện Python
└── README.md                        # Tài liệu hướng dẫn cài đặt và tái lập kết quả
```

---

## 3. Phân công nhiệm vụ (Nhóm 3 thành viên)

| Thành viên | File phụ trách | Nhiệm vụ chính |
| :--- | :--- | :--- |
| **Thành viên 1** | `src/config.py`<br>`src/step1_generate_and_profile.py` | Thiết kế Schema/Data Contract, sinh dữ liệu thô quy mô lớn vào tầng **Bronze** và thống kê tỷ lệ lỗi ban đầu (*Data Profiling*). |
| **Thành viên 2** | `src/step2_spark_processing.py` | Xây dựng Job **PySpark** lọc `NULL`, xóa bản ghi trùng (`dropDuplicates`), lọc ngoại lai, chuẩn hóa thời gian, lưu **Parquet** phân vùng vào tầng **Silver** và tính KPI tầng **Gold**. |
| **Thành viên 3** | `src/step3_benchmark_and_charts.py`<br>`run_pipeline.py` | Thiết kế và chạy 3 kịch bản thực nghiệm (Volume Scaling, CPU Parallelism, CSV vs Parquet) và xuất biểu đồ vào `output_charts/`. |

---

## 4. Hướng dẫn cài đặt và chạy (Khả năng tái lập)

### Bước 1: Cài đặt thư viện
```powershell
cd "c:\Users\vuvie\Documents\Big data\DoAn"
py -m pip install -r requirements.txt
```

### Bước 2: Chạy toàn bộ Pipeline (Từ Bước 1 đến Bước 3)
Chạy mặc định với `200,000` bản ghi để kiểm tra nhanh (mất khoảng 3–5 giây):
```powershell
py run_pipeline.py
```

Hoặc chạy thử tải lớn với `1,000,000` bản ghi:
```powershell
py run_pipeline.py --rows 1000000
```

### Bước 3: Chạy riêng lẻ từng phần của từng thành viên (nếu cần)
```powershell
# Thành viên 1: Sinh dữ liệu tầng Bronze
py src/step1_generate_and_profile.py --rows 500000

# Thành viên 2: Xử lý làm sạch và lưu xuống tầng Silver & Gold
py src/step2_spark_processing.py

# Thành viên 3: Chạy thực nghiệm Benchmark & xuất biểu đồ PNG
py src/step3_benchmark_and_charts.py
```

### Bước 4: Mở Web Dashboard trình bày kết quả (Mục 5.1 – 5.8 & So sánh Volume)
```powershell
py src/web_dashboard.py
```
Hoặc nháy đúp vào file `start_dashboard.bat`, trình duyệt sẽ tự động mở tại `http://localhost:8080`.


