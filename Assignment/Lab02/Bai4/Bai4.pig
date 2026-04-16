-- 1. Tải dữ liệu gốc (ngăn cách bởi dấu ;)
raw_data = LOAD 'hotel-review.csv' USING PigStorage(';') AS (id:chararray, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);
stopwords = LOAD 'stopwords.txt' AS (stopword:chararray);

-- 2. Tiền xử lý: Chuyển chữ thường và tách từ
lower_data = FOREACH raw_data GENERATE category, LOWER(sentiment) AS sentiment, LOWER(comment) AS comment_lower;
words_flat = FOREACH lower_data GENERATE category, sentiment, FLATTEN(TOKENIZE(comment_lower)) AS word;

-- 3. Loại bỏ Stopwords
joined_data = JOIN words_flat BY word LEFT OUTER, stopwords BY stopword USING 'replicated';
cleaned_words = FILTER joined_data BY stopwords::stopword IS NULL;
data_final = FOREACH cleaned_words GENERATE words_flat::category AS category, words_flat::sentiment AS sentiment, words_flat::word AS word;

-- 4. XỬ LÝ TÌM 5 TỪ TÍCH CỰC NHẤT THEO TỪNG CATEGORY
pos_data = FILTER data_final BY sentiment == 'positive';
pos_group = GROUP pos_data BY (category, word);
pos_counts = FOREACH pos_group GENERATE group.category AS category, group.word AS word, COUNT(pos_data) AS cnt;
-- Sắp xếp và lấy top 5 cho mỗi category
pos_ordered = GROUP pos_counts BY category;
top5_pos = FOREACH pos_ordered {
    sorted = ORDER pos_counts BY cnt DESC;
    lmt = LIMIT sorted 5;
    GENERATE group AS category, lmt AS top_words;
};

-- 5. XỬ LÝ TÌM 5 TỪ TIÊU CỰC NHẤT THEO TỪNG CATEGORY
neg_data = FILTER data_final BY sentiment == 'negative';
neg_group = GROUP neg_data BY (category, word);
neg_counts = FOREACH neg_group GENERATE group.category AS category, group.word AS word, COUNT(neg_data) AS cnt;
-- Sắp xếp và lấy top 5 cho mỗi category
neg_ordered = GROUP neg_counts BY category;
top5_neg = FOREACH neg_ordered {
    sorted = ORDER neg_counts BY cnt DESC;
    lmt = LIMIT sorted 5;
    GENERATE group AS category, lmt AS top_words;
};

-- 6. Hiển thị và Lưu kết quả
DUMP top5_pos;
DUMP top5_neg;

rmf KetQua_Bai4_Pos; rmf KetQua_Bai4_Neg;
STORE top5_pos INTO 'KetQua_Bai4_Pos' USING PigStorage(',');
STORE top5_neg INTO 'KetQua_Bai4_Neg' USING PigStorage(',');