-- 1. Load dữ liệu với đúng thứ tự cột của file thực tế
raw_data = LOAD '../dataset/hotel-review.csv' USING PigStorage(';') AS (
    id:chararray, 
    comment:chararray, 
    aspect:chararray,  -- Cột 3 là Aspect (GENERAL, QUALITY)
    category:chararray, -- Cột 4 là Category (HOTEL, SERVICE)
    sentiment:chararray
);

-- 2. Lọc bỏ dòng tiêu đề và dữ liệu lỗi (Tránh lỗi 2244)
clean_data = FILTER raw_data BY (id != 'id') AND (aspect IS NOT NULL) AND (sentiment IS NOT NULL);

-- 3. Tách dữ liệu Positive và Negative
pos_data = FILTER clean_data BY LOWER(sentiment) == 'positive';
neg_data = FILTER clean_data BY LOWER(sentiment) == 'negative';

-- 4. Xử lý Positive cao nhất
group_pos = GROUP pos_data BY aspect;
count_pos = FOREACH group_pos GENERATE group AS aspect, 'Positive' AS type, COUNT(pos_data) AS total;
ordered_pos = ORDER count_pos BY total DESC;
top_pos = LIMIT ordered_pos 1;

-- 5. Xử lý Negative cao nhất
group_neg = GROUP neg_data BY aspect;
count_neg = FOREACH group_neg GENERATE group AS aspect, 'Negative' AS type, COUNT(neg_data) AS total;
ordered_neg = ORDER count_neg BY total DESC;
top_neg = LIMIT ordered_neg 1;

-- 6. Gộp và Lưu kết quả
-- Lưu ý: Trong local mode dùng 'fs -rm -r -f' thay cho 'rmf' để an toàn hơn

final_output = UNION top_pos, top_neg;
STORE final_output INTO 'KetQua' USING PigStorage(',');

-- Hiển thị kết quả ra màn hình
DUMP final_output;