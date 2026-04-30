from pyspark import SparkContext

# Khởi tạo SparkContext
sc = SparkContext.getOrCreate()

# Ngưỡng lọc số lượt đánh giá tối thiểu (Theo yêu cầu bài 1 [cite: 1])
MIN_RATINGS = 5


# Bước 1: Đọc file movies.txt và tạo map (MovieID -> Title)
def parse_movie(line):
    parts = line.split(',')
    movie_id = parts[0]
    title = ','.join(parts[1:-1]) 
    return (movie_id, title)

movies_rdd = sc.textFile("../data/movies.txt").map(parse_movie)


# Bước 2: Đọc file ratings_1.txt & ratings_2.txt
ratings_1_rdd = sc.textFile("../data/ratings_1.txt")
ratings_2_rdd = sc.textFile("../data/ratings_2.txt")
ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

parsed_ratings = ratings_rdd.map(lambda line: line.split(',')) \
                            .map(lambda parts: (parts[1], (float(parts[2]), 1)))


# Bước 3 & 4: Tính điểm TB và lọc (>= 5 lượt đánh giá [cite: 1])
rating_totals = parsed_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
avg_ratings = rating_totals.mapValues(lambda x: (x[0] / x[1], x[1]))
filtered_ratings = avg_ratings.filter(lambda x: x[1][1] >= MIN_RATINGS)


# Bước 5: Tìm phim có điểm trung bình cao nhất
movie_stats = filtered_ratings.join(movies_rdd)
top_movie = movie_stats.max(key=lambda x: x[1][0][0])

# PHẦN MỚI: LƯU KẾT QUẢ RA FILE TEXT
output_file = "../result/bai1/result.txt"

# Thu thập dữ liệu từ các node về driver
results = movie_stats.collect()

with open(output_file, "w", encoding="utf-8") as f:
    f.write("--- DANH SACH THONG KE (>= 5 DANH GIA) ---\n")
    for movie_id, ((avg, count), title) in results:
        line = f"ID: {movie_id} | Phim: {title} | Diem TB: {avg:.2f} | So luot: {count}\n"
        f.write(line)
        print(line, end="") # Vừa in ra màn hình vừa lưu file

    f.write("\n" + "="*50 + "\n")
    f.write(f"PHIM CO DIEM TRUNG BINH CAO NHAT (>= {MIN_RATINGS} danh gia):\n")
    f.write(f"Ten phim: {top_movie[1][1]}\n")
    f.write(f"Diem trung binh: {top_movie[1][0][0]:.2f}\n")
    f.write(f"So luot danh gia: {top_movie[1][0][1]}\n")

print(f"\n--- Da luu ket qua vao file: {output_file} ---")