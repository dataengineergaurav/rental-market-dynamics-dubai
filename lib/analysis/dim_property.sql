-- =========================
-- dim_property
-- =========================
INSERT INTO dim_property (
    property_key,
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
)
SELECT 
    ROW_NUMBER() OVER () as property_key,
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
FROM (
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
    FROM rent_contracts
);
