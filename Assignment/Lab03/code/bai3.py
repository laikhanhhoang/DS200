from pyspark import SparkContext

# Khởi tạo SparkContext
sc = SparkContext.getOrCreate()

# ==========================================
# Bước 1: Tạo map (UserID -> Gender) từ file users.txt
# ==========================================
def parse_user(line):
    parts = line.split(',')
    user_id = parts[0]
    gender = parts[1]
    return (user_id, gender)

users_rdd = sc.textFile("../data/users.txt").map(parse_user)

# ==========================================
# (Phụ trợ): Đọc file movies.txt để lấy tên phim xuất ra cho đẹp
# Format: (MovieID, Title)
# ==========================================
def parse_movie(line):
    parts = line.split(',')
    movie_id = parts[0]
    title = ','.join(parts[1:-1]) 
    return (movie_id, title)

movies_rdd = sc.textFile("../data/movies.txt").map(parse_movie)

# ==========================================
# Bước 2: Join với ratings để thêm thông tin giới tính
# ==========================================
# 2.1 Đọc và gộp file ratings
ratings_1_rdd = sc.textFile("../data/ratings_1.txt")
ratings_2_rdd = sc.textFile("../data/ratings_2.txt")
ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

# 2.2 Map rating thành: (UserID, (MovieID, Rating))
# Lưu ý: Cần để UserID làm Key (đứng đầu) để có thể join với users_rdd
parsed_ratings = ratings_rdd.map(lambda line: line.split(',')) \
                            .map(lambda parts: (parts[0], (parts[1], float(parts[2]))))

# 2.3 Thực hiện Join
# Kết quả join có dạng: (UserID, ((MovieID, Rating), Gender))
joined_rdd = parsed_ratings.join(users_rdd)

# ==========================================
# Bước 3: Tính trung bình rating cho mỗi phim theo từng giới tính
# ==========================================
# 3.1 Chuyển đổi định dạng để chuẩn bị Reduce
# Ta lấy cặp (MovieID, Gender) làm Key mới. 
# Value là (Rating, 1) để chuẩn bị tính tổng và đếm
movie_gender_rdd = joined_rdd.map(lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1)))

# 3.2 Reduce để tính tổng điểm và số lượt đánh giá
# Kết quả: ((MovieID, Gender), (Tổng điểm, Tổng lượt))
totals_rdd = movie_gender_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# 3.3 Tính điểm trung bình
# Kết quả: ((MovieID, Gender), Điểm TB)
averages_rdd = totals_rdd.mapValues(lambda x: x[0] / x[1])

# ==========================================
# Bước 4: Join lấy tên phim, Sắp xếp và Lưu file
# ==========================================
# Đổi Key lại thành MovieID để join lấy tên phim: (MovieID, (Gender, Avg_Rating))
prep_for_movie_join = averages_rdd.map(lambda x: (x[0][0], (x[0][1], x[1])))

# Join lấy tên phim: (MovieID, ((Gender, Avg_Rating), Title))
final_stats = prep_for_movie_join.join(movies_rdd)

# Format lại cho gọn: (MovieID, Title, Gender, Avg_Rating)
clean_results = final_stats.map(lambda x: (int(x[0]), x[1][1], x[1][0][0], x[1][0][1]))

# Sắp xếp theo MovieID (tăng dần) rồi đến Giới tính
sorted_results = clean_results.sortBy(lambda x: (x[0], x[2]))

# === In và Lưu File ===
output_file = "../result/bai3/result.txt"
results = sorted_results.collect()

with open(output_file, "w", encoding="utf-8") as f:
    header = "--- DIEM TRUNG BINH PHIM THEO GIOI TINH ---\n"
    f.write(header)
    print(header, end="")
    
    current_movie_id = None
    for movie_id, title, gender, avg_rating in results:
        # In một dòng trống để ngăn cách giữa các phim cho dễ nhìn
        if current_movie_id != movie_id:
            f.write("-" * 40 + "\n")
            print("-" * 40)
            current_movie_id = movie_id
            
        line = f"ID: {movie_id:<4} | Phim: {title[:25]:<25} | Giới tính: {gender} | Điểm TB: {avg_rating:.2f}\n"
        f.write(line)
        print(line, end="")

print(f"\n--- Da luu ket qua vao file: {output_file} ---")