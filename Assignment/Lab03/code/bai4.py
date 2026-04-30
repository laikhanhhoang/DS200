from email.mime import base

from pyspark import SparkContext

# Khởi tạo SparkContext
sc = SparkContext.getOrCreate()
data_path = "/mnt/d/UIT/DS200_Materials/Assignment/Lab03/data"
result_path = "/mnt/d/UIT/DS200_Materials/Assignment/Lab03/result"


# ==========================================
# Hàm trợ giúp: Phân loại tuổi thành nhóm tuổi (Age Group)
# ==========================================
def get_age_group(age_str):
    try:
        age = int(age_str)
        if age < 18:
            return "Dưới 18"
        elif 18 <= age <= 24:
            return "18-24"
        elif 25 <= age <= 34:
            return "25-34"
        elif 35 <= age <= 44:
            return "35-44"
        elif 45 <= age <= 54:
            return "45-54"
        else:
            return "55+"
    except ValueError:
        return "Unknown"

# ==========================================
# Bước 1: Tạo map (UserID -> Age Group)
# File users.txt có schema: UserID, Gender, Age, Occupation, Zip-code
# ==========================================
def parse_user(line):
    parts = line.split(',')
    user_id = parts[0]
    age = parts[2] # Cột số 3 là Age (index 2)
    age_group = get_age_group(age)
    return (user_id, age_group)

users_rdd = sc.textFile(f"file://{data_path}/users.txt").map(parse_user)

# ==========================================
# (Phụ trợ) Đọc map Movies để lấy tên phim: (MovieID -> Title)
# ==========================================
def parse_movie(line):
    parts = line.split(',')
    movie_id = parts[0]
    title = ','.join(parts[1:-1])
    return (movie_id, title)

movies_rdd = sc.textFile(f"file://{data_path}/movies.txt").map(parse_movie)
# ==========================================
# Bước 2: Join với ratings để thêm nhóm tuổi
# ==========================================
# 2.1 Đọc ratings và đưa UserID lên làm Key: (UserID, (MovieID, Rating))
ratings_1_rdd = sc.textFile(f"file://{data_path}/ratings_1.txt")
ratings_2_rdd = sc.textFile(f"file://{data_path}/ratings_2.txt")
ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

parsed_ratings = ratings_rdd.map(lambda line: line.split(',')) \
                            .map(lambda parts: (parts[0], (parts[1], float(parts[2]))))

# 2.2 Join Ratings với Users
# Kết quả: (UserID, ((MovieID, Rating), AgeGroup))
joined_rdd = parsed_ratings.join(users_rdd)

# ==========================================
# Bước 3: Tính trung bình điểm đánh giá theo nhóm tuổi
# ==========================================
# 3.1 Đổi Key thành cặp (MovieID, AgeGroup), Value là (Rating, 1)
movie_age_rdd = joined_rdd.map(lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1)))

# 3.2 Tính tổng điểm và tổng số lượt đánh giá
totals_rdd = movie_age_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# 3.3 Tính điểm trung bình
averages_rdd = totals_rdd.mapValues(lambda x: x[0] / x[1])

# ==========================================
# Bước 4: Làm đẹp, Sắp xếp và Lưu file
# ==========================================
# Join với movies_rdd để lấy tên phim
prep_for_movie_join = averages_rdd.map(lambda x: (x[0][0], (x[0][1], x[1])))
final_stats = prep_for_movie_join.join(movies_rdd)

# Đưa về định dạng gọn gàng: (MovieID, Title, AgeGroup, AvgRating)
clean_results = final_stats.map(lambda x: (int(x[0]), x[1][1], x[1][0][0], x[1][0][1]))

# Sắp xếp theo ID Phim trước, sau đó theo Nhóm Tuổi để dễ đọc
sorted_results = clean_results.sortBy(lambda x: (x[0], x[2]))

# === In và Lưu File ===
output_file = f"{result_path}/bai4/result.txt"
results = sorted_results.collect()

with open(output_file, "w", encoding="utf-8") as f:
    header = "--- DIEM TRUNG BINH PHIM THEO NHOM TUOI ---\n"
    f.write(header)
    print(header, end="")
    
    current_movie_id = None
    for movie_id, title, age_group, avg_rating in results:
        # Ngăn cách giữa các phim bằng gạch ngang
        if current_movie_id != movie_id:
            f.write("-" * 50 + "\n")
            print("-" * 50)
            current_movie_id = movie_id
            
        line = f"ID: {movie_id:<4} | Phim: {title[:20]:<20} | Nhóm tuổi: {age_group:<8} | Điểm TB: {avg_rating:.2f}\n"
        f.write(line)
        print(line, end="")

print(f"\n--- Da luu ket qua vao file: {output_file} ---")