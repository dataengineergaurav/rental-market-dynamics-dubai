-- =========================
-- dim_location
-- =========================
INSERT INTO dim_location (
    location_key,
    area_id,
    area_name_en,
    area_name_ar,
    actual_area,
    nearest_landmark_en,
    nearest_metro_en,
    nearest_mall_en,
    nearest_landmark_ar,
    nearest_metro_ar,
    nearest_mall_ar
)
SELECT 
    ROW_NUMBER() OVER () as location_key,
    area_id,
    area_name_en,
    area_name_ar,
    actual_area,
    nearest_landmark_en,
    nearest_metro_en,
    nearest_mall_en,
    nearest_landmark_ar,
    nearest_metro_ar,
    nearest_mall_ar
FROM (
    SELECT DISTINCT
        area_id,
        area_name_en,
        area_name_ar,
        actual_area,
        nearest_landmark_en,
        nearest_metro_en,
        nearest_mall_en,
        nearest_landmark_ar,
        nearest_metro_ar,
        nearest_mall_ar
    FROM rent_contracts
);