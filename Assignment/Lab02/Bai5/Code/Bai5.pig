fs -rm -r -f KetQua


-- 1. Tải dữ liệu gốc (dấu phân cách ;)
raw_data = LOAD '../dataset/hotel-review.csv' USING PigStorage(';') AS (id:chararray, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);
stopwords = LOAD '../dataset/stopwords.txt' AS (stopword:chararray);

-- 2. Tiền xử lý: Chuyển chữ thường và tách từ
lower_data = FOREACH raw_data GENERATE category, LOWER(comment) AS comment_lower;
words_flat = FOREACH lower_data GENERATE category, FLATTEN(TOKENIZE(comment_lower)) AS word;

-- 3. Loại bỏ Stopwords
joined_data = JOIN words_flat BY word LEFT OUTER, stopwords BY stopword USING 'replicated';
cleaned_data = FILTER joined_data BY stopwords::stopword IS NULL;
data_final = FOREACH cleaned_data GENERATE words_flat::category AS category, words_flat::word AS word;

-- 4. Thống kê tần suất từ theo từng Category
group_cat_word = GROUP data_final BY (category, word);
word_counts = FOREACH group_cat_word GENERATE group.category AS category, group.word AS word, COUNT(data_final) AS cnt;

-- 5. Lấy Top 5 từ liên quan nhất (xuất hiện nhiều nhất) cho mỗi Category
group_by_cat = GROUP word_counts BY category;
top5_related = FOREACH group_by_cat {
    sorted = ORDER word_counts BY cnt DESC;
    lmt = LIMIT sorted 5;
    GENERATE group AS category, lmt AS related_words;
};

-- 6. Hiển thị và Lưu kết quả
DUMP top5_related;

rmf KetQua_Bai5;
STORE top5_related INTO 'KetQua' USING PigStorage(',');