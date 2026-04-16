-- 1. Tải dữ liệu và Stopwords
raw_data = LOAD 'hotel-review.csv' USING PigStorage(';') AS (id:chararray, comment:chararray);
stopwords = LOAD 'stopwords.txt' AS (stopword:chararray);

-- 2. Chuyển về chữ thường (Lowercase)
lower_data = FOREACH raw_data GENERATE id, LOWER(comment) AS comment_lower;

-- 3. Tách dòng thành dãy các từ (Tokenize)
-- TOKENIZE tách theo khoảng trắng và FLATTEN để đưa mỗi từ về 1 dòng
words_data = FOREACH lower_data GENERATE id, FLATTEN(TOKENIZE(comment_lower)) AS word;

-- 4. Loại bỏ stop word (Sử dụng Join để lọc)
joined_data = JOIN words_data BY word LEFT OUTER, stopwords BY stopword USING 'replicated';
filtered_words = FILTER joined_data BY stopwords::stopword IS NULL;

-- 5. Định dạng kết quả cuối cùng (ID, từ đã lọc)
final_result = FOREACH filtered_words GENERATE words_data::id, words_data::word;

-- 6. LƯU KẾT QUẢ VÀO THƯ MỤC
-- Lưu ý: Thư mục KetQuaBai1 không được tồn tại trước khi chạy lệnh này
STORE final_result INTO 'KetQuaBai1' USING PigStorage(',');