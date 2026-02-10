-- =========================
-- dim_property
-- =========================
with dim_property as (
SELECT DISTINCT
	ejari_bus_property_type_id,
	ejari_property_type_id,
	ejari_bus_property_type_en,
    ejari_bus_property_type_ar,
    ejari_property_type_en,
    ejari_property_type_ar,
    ejari_property_sub_type_en,
    ejari_property_sub_type_ar,
    property_usage_en,
    property_usage_ar
FROM rent_contracts_df 
)
select * from dim_property;
