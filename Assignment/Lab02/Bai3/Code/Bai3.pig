fs -rm -r -f KetQua

-- 1. Đổi thành dấu PHẨY và đúng thứ tự cột
raw_data = LOAD '../dataset/hotel-review.csv' USING PigStorage(';') AS (
    id:chararray, 
    comment:chararray, 
    category:chararray, 
    aspect:chararray, 
    sentiment:chararray
);

-- 1.1. Loại bỏ dòng tiêu đề
raw_data = FILTER raw_data BY id != 'id';

-- Check raw_data
--STORE raw_data INTO 'KetQua/raw_data' USING PigStorage(',');


-- 2. Lọc ra các dòng Tích cực và Tiêu cực
-- Lưu ý: Pig phân biệt hoa thường, nên để chắc chắn ta dùng LOWER
pos_data = FILTER raw_data BY LOWER(sentiment) == 'positive';
neg_data = FILTER raw_data BY LOWER(sentiment) == 'negative';

-- Check pos_data và neg_data
--STORE pos_data INTO 'KetQua/pos_data'   USING PigStorage(',');
--STORE neg_data INTO 'KetQua/neg_data'   USING PigStorage(',');


-- 3. Tìm Aspect có nhiều Positive nhất
group_pos = GROUP pos_data BY aspect;
count_pos = FOREACH group_pos GENERATE 
                group AS aspect, 
                (long)COUNT(pos_data) AS total;
ordered_pos = ORDER count_pos BY total DESC;
ranked_pos = RANK ordered_pos;
top_one_ranked = FILTER ranked_pos BY $0 == 1;
top_pos_final = FOREACH top_one_ranked GENERATE aspect, total;

-- Lưu lại khía cạnh nhận nhiều đánh giá tích cực nhất
STORE top_pos_final INTO 'KetQua/Aspect_PositiveSentiment_NhieuNhat' USING PigStorage(',');




-- 4. Tìm Aspect có nhiều Negative nhất
group_neg = GROUP neg_data BY aspect;
count_neg = FOREACH group_neg GENERATE 
                group AS aspect, 
                (long)COUNT(neg_data) AS total;

ordered_neg = ORDER count_neg BY total DESC;
ranked_neg  = RANK ordered_neg;
top_neg_ranked = FILTER ranked_neg BY $0 == 1;
top_neg_final = FOREACH top_neg_ranked GENERATE aspect, total;

-- Lưu lại khía cạnh nhận nhiều đánh giá tiêu cực nhất
STORE top_neg_final INTO 'KetQua/Aspect_NegativeSentiment_NhieuNhat' USING PigStorage(',');