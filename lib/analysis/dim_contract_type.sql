-- =========================
-- dim_contract_type
-- =========================
INSERT INTO dim_contract_type (
    contract_type_key,
    contract_reg_type_id,
    contract_id,
    contract_reg_type_en,
    contract_reg_type_ar
)
SELECT 
    ROW_NUMBER() OVER () as contract_type_key,
    contract_reg_type_id,
    contract_id,
    contract_reg_type_en,
    contract_reg_type_ar
FROM (
    SELECT DISTINCT
        contract_reg_type_id,
        contract_id,
        contract_reg_type_en,
        contract_reg_type_ar
    FROM rent_contracts
);
