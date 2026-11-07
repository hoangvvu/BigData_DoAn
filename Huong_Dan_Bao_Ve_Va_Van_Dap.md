# Cẩm Nang Thuyết Trình, Demo & Trả Lời Vấn Đáp (Nhóm 3 Thành Viên)

Tài liệu này giúp 3 thành viên trong nhóm nắm vững kịch bản trình bày, các thao tác Demo trực tiếp trên `http://localhost:8080`, và bộ câu trả lời cho các câu hỏi vấn đáp thường gặp của giảng viên (theo đúng yêu cầu Mục 8 trong đề bài: *"Mỗi thành viên phải hiểu rõ và giải thích được hệ thống của mình"*).

---

## PHẦN 1: KỊCH BẢN THUYẾT TRÌNH & DEMO (10 - 12 PHÚT)

### 1. Thành viên 1 (Phụ trách Data & Storage - Tầng Bronze): Trình bày 3 phút đầu
- **Nội dung trình bày:**
  1. Giới thiệu bài toán xử lý dữ liệu giao dịch Thương mại điện tử và phạm vi quy trình nhóm chọn: **`Storage → Processing`**.
  2. Giải thích **2 đặc trưng Big Data** chính của bài toán:
     - **Volume (Khối lượng lớn):** Hàng trăm nghìn đến hàng triệu bản ghi giao dịch trong mỗi lô xử lý (Batch).
     - **Veracity (Độ xác thực / Nhiễu dữ liệu):** Dữ liệu thô từ các nguồn thu thập chứa **~12.09% bản ghi lỗi** (4% trùng lặp `Duplicate`, 4% khuyết thiếu `NULL`, 2.5% giá trị âm ngoại lai, 1.5% lỗi định dạng thời gian).
  3. Trình bày **Data Contract (Hợp đồng dữ liệu 10 cột)** trong file `src/config.py` và cơ chế sinh/kiểm định dữ liệu đầu vào (`src/step1_generate_and_profile.py`).

### 2. Thành viên 2 (Phụ trách Core PySpark - Tầng Silver & Gold): Trình bày 4 phút giữa
- **Nội dung trình bày:**
  1. Giải thích kiến trúc **Data Lake Medallion (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**.
  2. Giải thích luồng xử lý phân tán bằng **Apache Spark (`src/step2_spark_processing.py`)**:
     - Bước 1: Lọc bỏ `NULL` ở các cột trọng yếu (`dropna`).
     - Bước 2: Khử trùng lặp theo khóa chính `transaction_id` (`dropDuplicates`).
     - Bước 3: Lọc bỏ giá trị ngoại lai (`price > 0` và `quantity > 0`).
     - Bước 4: Chuẩn hóa chuỗi thời gian sang `Timestamp`, trích xuất cột phân vùng `year`, `month` và tính cột doanh thu `total_amount = price * quantity`.
  3. Giải thích lý do lưu tầng Silver dưới định dạng **Parquet phân vùng (`year=2025/month=01..12`)** và tổng hợp bảng KPI doanh thu tại tầng Gold.

### 3. Thành viên 3 (Phụ trách Thực nghiệm Benchmark & Web Dashboard): Trình bày 4 phút cuối
- **Thao tác Demo trực tiếp:**
  1. Mở trình duyệt tại **`http://localhost:8080`** (chạy từ `start_dashboard.bat`).
  2. Chỉ vào 5 thẻ KPI trên cùng và bảng đối chiếu bản ghi lỗi (gắn nhãn đỏ) vs. bản ghi hợp lệ (gắn nhãn xanh).
  3. Bấm nút **"▶ Chạy lại Pipeline ngay"** trên góc phải màn hình Web để giảng viên thấy hệ thống chạy thực tế và cập nhật số liệu trực tiếp.
  4. Giải thích **3 biểu đồ Thực nghiệm Benchmark**:
     - **Biểu đồ 1 (Volume Scaling):** Khi dữ liệu tăng lên hàng triệu dòng, xử lý phân tán nhanh gấp hơn **4.5 lần** xử lý tuần tự 1 luồng.
     - **Biểu đồ 2 (CPU Parallelism):** Tăng số nhân từ `1 core` lên `8 cores` giúp giảm thời gian chạy từ `33.24s` xuống `7.31s` (tăng tốc **4.55x**).
     - **Biểu đồ 3 (Storage Optimization):** Định dạng Parquet phân vùng giảm **69.16%** dung lượng ổ cứng và đọc nhanh gấp **7.2 lần** so với CSV.

---

## PHẦN 2: BỘ CÂU HỎI VẤN ĐÁP THƯỜNG GẶP CỦA GIẢNG VIÊN & CÁCH TRẢ LỜI

### Câu 1 (Hỏi Thành viên 1): *"Tại sao nhóm không làm phần Kafka trước đó mà lại bắt đầu từ file CSV trong tầng Bronze?"*
- **Trả lời:**
  > Thưa thầy/cô, theo yêu cầu đồ án, nhóm em phụ trách quy trình con **`Storage → Processing`**. Để hệ thống vừa hoạt động độc lập (không phụ thuộc tiến độ nhóm làm *Ingestion → Storage*), vừa sẵn sàng ghép nối với nhóm trước, nhóm em áp dụng thiết kế **Data Contract (Hợp đồng dữ liệu)** tại file `src/config.py`. Kết quả cuối cùng mà nhóm *Ingestion → Storage* bàn giao lại chính là các tệp dữ liệu thô nằm trong tầng Bronze. Vì vậy, nhóm em xây dựng module `step1_generate_and_profile.py` đóng vai trò tạo dữ liệu chuẩn theo đúng Schema Contract đó để kiểm thử tải lớn độc lập. Khi ghép với nhóm trước, nhóm em chỉ cần trỏ đường dẫn `BRONZE_DIR` vào thư mục xuất của nhóm trước là chạy được ngay.

### Câu 2 (Hỏi Thành viên 2): *"Tại sao ở tầng Silver nhóm lại chọn định dạng Parquet và phân vùng theo `year`, `month` mà không giữ nguyên CSV hay phân vùng theo `transaction_id`?"*
- **Trả lời:**
  > Thưa thầy/cô, có 2 lý do kỹ thuật rất quan trọng ạ:
  > 1. **Về định dạng Parquet vs. CSV:** CSV lưu theo dòng (Row-based) nên khi chỉ cần tính tổng cột `price`, hệ thống vẫn phải đọc toàn bộ 10 cột từ đĩa. Ngược lại, **Parquet lưu theo cột (Columnar)**, cho phép chỉ đọc đúng cột cần thiết (*Column Projection*) và nén các giá trị cùng kiểu dữ liệu rất tốt. Thực nghiệm của nhóm chứng minh dung lượng giảm từ `9.37 MB` xuống `2.89 MB` (**tiết kiệm 69.16%**).
  > 2. **Về chọn cột phân vùng (`year`, `month`):** Các truy vấn phân tích thương mại điện tử hầu hết đều lọc theo thời gian (ví dụ báo cáo tháng 5/2025). Phân vùng theo `year/month` giúp Spark tận dụng cơ chế **Partition Pruning** (chỉ đọc đúng thư mục tháng 5, bỏ qua 11 tháng còn lại). Nếu phân vùng theo cột có quá nhiều giá trị riêng biệt (High Cardinality) như `transaction_id` hay `event_time` đến từng giây, hệ thống sẽ mắc lỗi **Small File Problem** (sinh ra hàng trăm nghìn thư mục và file nhỏ vài KB, làm sập Metadata của NameNode/Storage).

### Câu 3 (Hỏi Thành viên 2): *"Trong PySpark, thứ tự các bước làm sạch (`dropna`, `dropDuplicates`, `filter`) có ảnh hưởng đến hiệu năng không?"*
- **Trả lời:**
  > Dạ có ảnh hưởng rất lớn ạ! Trong file `src/step2_spark_processing.py`, nhóm em đặt `dropna` và `filter` (các phép biến đổi **Narrow Transformation** - xử lý cục bộ trên từng phân vùng, không cần truyền dữ liệu qua mạng) kết hợp với `dropDuplicates` (phép biến đổi **Wide Transformation** - cần *Shuffle* dữ liệu giữa các phân vùng để gom các mã `transaction_id` giống nhau về cùng một nơi). Việc loại bỏ các dòng rỗng `NULL` trước giúp giảm số lượng bản ghi phải tham gia vào pha *Shuffle* của `dropDuplicates`, từ đó giảm chi phí I/O và tăng tốc độ xử lý.

### Câu 4 (Hỏi Thành viên 3): *"Ở Thí nghiệm 1, tại sao ở mốc dữ liệu nhỏ (100K dòng), khoảng cách giữa Spark và xử lý đơn luồng không quá lớn, nhưng càng lên 2M - 5M dòng thì Spark lại bỏ xa?"*
- **Trả lời:**
  > Thưa thầy/cô, ở mốc dữ liệu nhỏ, công cụ phân tán như Spark luôn tốn một khoảng thời gian cố định ban đầu (~1.0 – 1.5 giây) để khởi tạo `SparkSession`, lập kế hoạch thực thi DAG (*Catalyst Optimizer*) và chia task cho các luồng. Nhưng khi quy mô dữ liệu tăng lên hàng triệu dòng, xử lý đơn luồng bị nghẽn CPU và chịu phạt bộ nhớ RAM nên thời gian tăng vọt, trong khi xử lý phân tán chia đều tải cho nhiều nhân CPU nên đường biểu diễn thời gian phẳng hơn rất nhiều.

