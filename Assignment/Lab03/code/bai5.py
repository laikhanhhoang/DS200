from pyspark import SparkContext

# Khởi tạo SparkContext
sc = SparkContext.getOrCreate()

# ==========================================
# Bước 1.1: Tạo dictionary từ occupation.txt (OccID -> Tên nghề)
# Schema: ID, Occupation
# ==========================================
def parse_occupation(line):
    parts = line.split(',')
    return (parts[0], parts[1])

# Tạo và Broadcast từ điển nghề nghiệp
occ_dict = sc.textFile("../data/occupation.txt").map(parse_occupation).collectAsMap()
broadcast_occ = sc.broadcast(occ_dict)

# ==========================================
# Bước 1.2: Tạo dictionary từ users.txt (UserID -> OccID)
# Schema: UserID, Gender, Age, Occupation, Zip-code
# ==========================================
def parse_user(line):
    parts = line.split(',')
    user_id = parts[0]
    occ_id = parts[3] # Cột thứ 4 chứa ID của nghề nghiệp
    return (user_id, occ_id)

# Tạo và Broadcast từ điển user
user_dict = sc.textFile("../data/users.txt").map(parse_user).collectAsMap()
broadcast_user = sc.broadcast(user_dict)

# ==========================================
# Bước 2 & 3: Xử lý file Ratings và gán Tên Nghề Nghiệp
# ==========================================
ratings_1_rdd = sc.textFile("../data/ratings_1.txt")
ratings_2_rdd = sc.textFile("../data/ratings_2.txt")
ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

def map_rating_to_occupation(line):
    parts = line.split(',')
    user_id = parts[0]
    rating = float(parts[2])
    
    # Bước nhảy 1: Lấy ID nghề nghiệp của User (nếu không có thì trả về "-1")
    occ_id = broadcast_user.value.get(user_id, "-1")
    
    # Bước nhảy 2: Dịch ID nghề nghiệp sang Tên nghề (nếu không có thì trả về "Unknown")
    occ_name = broadcast_occ.value.get(occ_id, "Unknown")
    
    # Phát hành cặp key-value
    return (occ_name, (rating, 1))

occ_ratings_rdd = ratings_rdd.map(map_rating_to_occupation)

# ==========================================
# Bước 4: Reduce tính tổng và tính trung bình
# ==========================================
# Tính tổng điểm và tổng số lượt
occ_totals_rdd = occ_ratings_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính trung bình: Kết quả là (Occupation, (Điểm TB, Tổng số lượt))
occ_averages_rdd = occ_totals_rdd.mapValues(lambda x: (x[0] / x[1], x[1]))

# Sắp xếp theo Điểm TB giảm dần
sorted_occ_stats = occ_averages_rdd.sortBy(lambda x: x[1][0], ascending=False)

# ==========================================
# In và Lưu Kết Quả Ra File
# ==========================================
output_file = "../result/bai5/result.txt"
results = sorted_occ_stats.collect()

with open(output_file, "w", encoding="utf-8") as f:
    header = "--- THONG KE DANH GIA THEO NGHE NGHIEP ---\n"
    f.write(header)
    print(header, end="")
    
    for occ_name, (avg_rating, count) in results:
        line = f"Nghe nghiep: {occ_name:<15} | Diem TB: {avg_rating:.2f} | Tong luot danh gia: {count}\n"
        f.write(line)
        print(line, end="")

print(f"\n--- Da luu ket qua vao file: {output_file} ---")