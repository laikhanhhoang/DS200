-- ==========================================================
-- TỰ ĐỘNG XÓA KẾT QUẢ CŨ (Để không bị lỗi STORE)
-- ==========================================================
fs -rm -r -f KetQua


-- ==========================================================
-- LOAD DATA
-- ==========================================================
data = LOAD '../dataset/hotel-review.csv' USING PigStorage(';') 
AS (
    id:chararray, 
    comment:chararray, 
    aspect:chararray, 
    category:chararray, 
    sentiment:chararray
);

-- LOAD KẾT QUẢ BÀI 1
cleaned_words = LOAD '../Bai1/KetQua' USING PigStorage(',') 
AS (id:chararray, word:chararray);

-- ==========================================================
-- YÊU CẦU 1: Thống kê tần số từ > 500 lần
-- ==========================================================
group_words = GROUP cleaned_words BY word;

word_counts = FOREACH group_words 
GENERATE 
    group AS word, 
    COUNT(cleaned_words) AS count;

high_freq_words = FILTER word_counts BY count > 500;

-- ==========================================================
-- YÊU CẦU 2: Thống kê số bình luận theo Category
-- ==========================================================
temp_category = FOREACH data GENERATE id, category;

unique_category = DISTINCT temp_category;

group_category = GROUP unique_category BY category;

category_counts = FOREACH group_category 
GENERATE 
    group AS category, 
    COUNT(unique_category) AS num_comments;

-- ==========================================================
-- YÊU CẦU 3: Thống kê số bình luận theo Aspect
-- ==========================================================
temp_aspect = FOREACH data GENERATE id, aspect;

unique_aspect = DISTINCT temp_aspect;

group_aspect = GROUP unique_aspect BY aspect;

aspect_counts = FOREACH group_aspect 
GENERATE 
    group AS aspect, 
    COUNT(unique_aspect) AS num_comments;

-- ==========================================================
-- LƯU KẾT QUẢ
-- ==========================================================
STORE high_freq_words INTO 'KetQua/Tu_SoLanXuatHien_Tren500' USING PigStorage(',');

STORE category_counts INTO 'KetQua/BinhLuan_SoLuong_Cate' USING PigStorage(',');

STORE aspect_counts   INTO 'KetQua/BinhLuan_SoLuong_Aspect'   USING PigStorage(',');