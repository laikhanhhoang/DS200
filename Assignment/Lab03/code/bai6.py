from pyspark import SparkContext
from datetime import datetime

# Khởi tạo SparkContext
sc = SparkContext.getOrCreate()
data_path = "/mnt/d/UIT/DS200_Materials/Assignment/Lab03/data"
result_path = "/mnt/d/UIT/DS200_Materials/Assignment/Lab03/result"

# ==========================================
# Hàm trợ giúp: Chuyển đổi Timestamp (Unix) thành Năm (Year)
# ==========================================
def get_year_from_timestamp(ts_str):
    try:
        # Chuyển chuỗi thành số nguyên, sau đó dùng fromtimestamp để lấy đối tượng ngày giờ
        timestamp = int(ts_str)
        dt_object = datetime.fromtimestamp(timestamp)
        return dt_object.year
    except ValueError:
        return -1 # Trả về -1 nếu dữ liệu bị lỗi

# ==========================================
# Bước 1: Đọc dữ liệu ratings (từ cả ratings_1.txt và ratings_2.txt)
# Schema: UserID, MovieID, Rating, Timestamp
# ==========================================
ratings_1_rdd = sc.textFile(f"{data_path}/ratings_1.txt")
ratings_2_rdd = sc.textFile(f"{data_path}/ratings_2.txt")
ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

# ==========================================
# Bước 2 & 3: Phát hành cặp (Năm, (Rating, 1))
# ==========================================
def map_rating_to_year(line):
    parts = line.split(',')
    rating = float(parts[2])     # Cột 3 là Rating
    timestamp = parts[3].strip() # Cột 4 là Timestamp (loại bỏ khoảng trắng/newline nếu có)
    
    # Sử dụng hàm trợ giúp để lấy năm
    year = get_year_from_timestamp(timestamp)
    
    return (year, (rating, 1))

year_ratings_rdd = ratings_rdd.map(map_rating_to_year)

# Lọc bỏ các dòng lỗi (nếu có trả về -1 từ hàm get_year)
year_ratings_rdd = year_ratings_rdd.filter(lambda x: x[0] != -1)

# ==========================================
# Bước 4: Reduce để tính tổng và tính trung bình
# ==========================================
# 4.1 Tính tổng điểm và tổng số lượt đánh giá cho mỗi năm
year_totals_rdd = year_ratings_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# 4.2 Tính trung bình rating. 
# Format kết quả: (Year, (AvgRating, TotalCount))
year_averages_rdd = year_totals_rdd.mapValues(lambda x: (x[0] / x[1], x[1]))

# 4.3 Sắp xếp kết quả theo Năm (tăng dần) để dễ theo dõi xu hướng
sorted_year_stats = year_averages_rdd.sortByKey(ascending=True)

# ==========================================
# In và Lưu Kết Quả Ra File
# ==========================================
output_file = f"{result_path}/bai6/result.txt"
results = sorted_year_stats.collect()

with open(output_file, "w", encoding="utf-8") as f:
    header = "--- THONG KE DANH GIA THEO NAM ---\n"
    f.write(header)
    print(header, end="")
    
    for year, (avg_rating, count) in results:
        line = f"Năm: {year} | Điểm TB: {avg_rating:.2f} | Tổng số lượt đánh giá: {count}\n"
        f.write(line)
        print(line, end="")

print(f"\n--- Da luu ket qua vao file: {output_file} ---")