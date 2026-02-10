-- =========================
-- dim_tenant
-- =========================
with dim_tenant as (
    SELECT DISTINCT
        tenant_type_id,
        tenant_type_en,
        tenant_type_ar
    FROM rent_contracts_df;
)
select * from dim_tenant;
