-- 1. Tải dữ liệu (Sử dụng dấu chấm phẩy làm vật ngăn cách)
raw_data = LOAD 'hotel-review.csv' USING PigStorage(';') AS (
    id:chararray, 
    comment:chararray, 
    category:chararray, 
    aspect:chararray, 
    sentiment:chararray
);

-- 2. Lọc ra các dòng Tích cực và Tiêu cực
-- Lưu ý: Pig phân biệt hoa thường, nên để chắc chắn ta dùng LOWER
pos_data = FILTER raw_data BY LOWER(sentiment) == 'positive';
neg_data = FILTER raw_data BY LOWER(sentiment) == 'negative';

-- 3. Tìm Aspect có nhiều Positive nhất
group_pos = GROUP pos_data BY aspect;
count_pos = FOREACH group_pos GENERATE group AS aspect, COUNT(pos_data) AS total;
ordered_pos = ORDER count_pos BY total DESC;
top_pos = LIMIT ordered_pos 1;

-- 4. Tìm Aspect có nhiều Negative nhất
group_neg = GROUP neg_data BY aspect;
count_neg = FOREACH group_neg GENERATE group AS aspect, COUNT(neg_data) AS total;
ordered_neg = ORDER count_neg BY total DESC;
top_neg = LIMIT ordered_neg 1;

-- 5. Xuất kết quả ra màn hình để xem nhanh
DESCRIBE top_pos;
DUMP top_pos;
DUMP top_neg;

-- 6. Lưu kết quả
rmf KetQua_Bai3;
final_output = UNION top_pos, top_neg; -- Gộp cả 2 kết quả vào 1 file cho tiện
STORE final_output INTO 'KetQuaBai3' USING PigStorage(',');