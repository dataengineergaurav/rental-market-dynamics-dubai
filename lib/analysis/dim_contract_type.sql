-- =========================
-- dim_contract_type
-- =========================

with dim_contract_type as (
    SELECT DISTINCT
        contract_reg_type_id,
        contract_id,
        contract_reg_type_en,
        contract_reg_type_ar
    FROM rent_contracts_df
)
select * from dim_contract_type;
