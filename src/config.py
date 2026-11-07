"""
config.py - Cấu hình đường dẫn Data Lake và Data Contract chung cho cả nhóm 3 thành viên.
Quy trình: Storage (Bronze) -> Processing (PySpark) -> Storage tối ưu (Silver/Gold Parquet)
"""

from pathlib import Path

# Thư mục gốc của đồ án
BASE_DIR = Path(__file__).resolve().parent.parent

# Cấu trúc Data Lake theo kiến trúc Medallion (Bronze -> Silver -> Gold)
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze_raw"          # Dữ liệu thô đầu vào (CSV/JSON chưa làm sạch)
SILVER_DIR = DATA_DIR / "silver_cleaned"      # Dữ liệu sạch (Parquet phân vùng theo year/month)
GOLD_DIR = DATA_DIR / "gold_aggregated"       # Dữ liệu tổng hợp KPI nghiệp vụ (Parquet/CSV)

# Thư mục lưu kết quả thực nghiệm, log và biểu đồ cho báo cáo
REPORTS_DIR = BASE_DIR / "output_reports"
CHARTS_DIR = BASE_DIR / "output_charts"

# Tự động tạo các thư mục nếu chưa tồn tại
for directory in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, REPORTS_DIR, CHARTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# File dữ liệu mặc định ở tầng Bronze
DEFAULT_RAW_CSV = BRONZE_DIR / "transactions_raw.csv"

# ==============================================================================
# DATA CONTRACT (HỢP ĐỒNG DỮ LIỆU ĐẦU VÀO & ĐẦU RA)
# ==============================================================================
SCHEMA_CONTRACT = {
    "transaction_id": "STRING (Khóa chính giao dịch, VD: TXN10000001)",
    "user_id": "STRING (Mã khách hàng, VD: USR004521)",
    "product_id": "STRING (Mã sản phẩm, VD: PRD00125)",
    "category": "STRING (Ngành hàng: Electronics, Fashion, Home, Beauty, Books, Sports)",
    "price": "DOUBLE (Đơn giá sản phẩm > 0, đơn vị USD)",
    "quantity": "INT (Số lượng mua > 0)",
    "event_time": "TIMESTAMP (Thời gian phát sinh giao dịch: YYYY-MM-DD HH:MM:SS)",
    "payment_method": "STRING (Credit Card, E-Wallet, COD, Bank Transfer)",
    "city": "STRING (Ha Noi, Ho Chi Minh, Da Nang, Can Tho, Hai Phong)",
    "status": "STRING (COMPLETED, PENDING, CANCELLED, REFUNDED)",
}

CATEGORIES = ["Electronics", "Fashion", "Home & Living", "Beauty", "Books", "Sports"]
PAYMENT_METHODS = ["Credit Card", "E-Wallet", "COD", "Bank Transfer"]
CITIES = ["Ho Chi Minh", "Ha Noi", "Da Nang", "Hai Phong", "Can Tho"]
STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]

