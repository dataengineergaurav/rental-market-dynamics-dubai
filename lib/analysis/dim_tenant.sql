-- =========================
-- dim_tenant
-- =========================
INSERT INTO dim_tenant (
    tenant_key,
    tenant_type_id,
    tenant_type_en,
    tenant_type_ar
)
SELECT 
    ROW_NUMBER() OVER () as tenant_key,
    tenant_type_id,
    tenant_type_en,
    tenant_type_ar
FROM (
    SELECT DISTINCT
        tenant_type_id,
        tenant_type_en,
        tenant_type_ar
    FROM rent_contracts
);
