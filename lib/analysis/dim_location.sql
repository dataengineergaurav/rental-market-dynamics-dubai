-- =========================
-- dim_location
-- =========================
with dim_location as (
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
    from rent_contracts_df
)
select * from dim_location;