from pyspark import SparkContext

# Khởi tạo SparkContext
sc = SparkContext.getOrCreate()

# ==========================================
# Bước 1: Tạo map (MovieID -> List of Genres)
# ==========================================
def parse_movie_genres(line):
    parts = line.split(',')
    movie_id = parts[0]
    # Cột cuối cùng là chuỗi chứa các thể loại phân cách bằng dấu '|'
    genres_str = parts[-1] 
    # Tách chuỗi thành danh sách (List) các thể loại
    genres_list = genres_str.split('|') 
    return (movie_id, genres_list)

movies_rdd = sc.textFile("../data/movies.txt").map(parse_movie_genres)

# ==========================================
# Bước 2: Map từ MovieID -> Rating -> (Genre, Rating)
# ==========================================
# 2.1. Đọc và parse RDD ratings thành (MovieID, (Rating, 1))
ratings_1_rdd = sc.textFile("../data/ratings_1.txt")
ratings_2_rdd = sc.textFile("../data/ratings_2.txt")
ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

parsed_ratings = ratings_rdd.map(lambda line: line.split(',')) \
                            .map(lambda parts: (parts[1], (float(parts[2]), 1)))

# 2.2. Join Movies và Ratings
# Kết quả sau khi join: (MovieID, ( [Genre1, Genre2], (Rating, 1) ))
joined_rdd = movies_rdd.join(parsed_ratings)

# 2.3. FlatMap để tách từng thể loại ra thành các bản ghi riêng biệt
# Format đầu ra: (Genre, (Rating, 1))
def explode_genres(record):
    # record[1][0] là danh sách thể loại
    # record[1][1] là tuple (Rating, 1)
    genres_list = record[1][0]
    rating_data = record[1][1]
    
    results = []
    for genre in genres_list:
        results.append((genre, rating_data))
    return results

genre_ratings_rdd = joined_rdd.flatMap(explode_genres)

# ==========================================
# Bước 3: Tính trung bình điểm đánh giá cho từng thể loại
# ==========================================
# 3.1. Tính tổng điểm và tổng số lượt đánh giá cho mỗi thể loại
genre_totals = genre_ratings_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# 3.2. Tính điểm trung bình (Tổng điểm / Tổng lượt)
genre_averages = genre_totals.mapValues(lambda x: x[0] / x[1])

# 3.3 Sắp xếp kết quả theo điểm trung bình giảm dần để dễ quan sát
sorted_genres = genre_averages.sortBy(lambda x: x[1], ascending=False)

# ==========================================
# In và Lưu Kết Quả Ra File
# ==========================================
output_file = "../result/bai2/result.txt"
results = sorted_genres.collect()

with open(output_file, "w", encoding="utf-8") as f:
    header = "--- DIEM TRUNG BINH THEO TUNG THE LOAI ---\n"
    f.write(header)
    print(header, end="")
    
    for genre, avg_rating in results:
        line = f"The loai: {genre:<15} | Diem TB: {avg_rating:.2f}\n"
        f.write(line)
        print(line, end="")

print(f"\n--- Da luu ket qua vao file: {output_file} ---")