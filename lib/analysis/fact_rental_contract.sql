-- =========================
-- fact_rental_contract
-- =========================
INSERT INTO fact_rent_contract (
    rent_contract_key,
    contract_type_key,
    property_key,
    project_key,
    location_key,
    tenant_key,
    contract_start_date,
    contract_end_date,
    annual_amount,
    contract_amount,
    no_of_prop,
    line_number,
    is_free_hold
)
SELECT 
    ROW_NUMBER() OVER () as rent_contract_key,
    dct.contract_type_key,
    dp.property_key,
    dprj.project_key,
    dl.location_key, 
    dt.tenant_key,
    rc.contract_start_date,
    rc.contract_end_date,
    rc.annual_amount,
    rc.contract_amount,
    rc.no_of_prop,
    rc.line_number,
    rc.is_free_hold
FROM rent_contracts rc
JOIN dim_contract_type dct
    ON rc.contract_reg_type_id = dct.contract_reg_type_id
JOIN dim_project dprj
    ON rc.project_number = dprj.project_number
JOIN dim_property dp
    ON rc.ejari_bus_property_type_id = dp.ejari_bus_property_type_id  
    AND rc.ejari_property_type_id = dp.ejari_property_type_id
JOIN dim_location dl
    ON rc.area_id = dl.area_id  
JOIN dim_tenant dt
    ON rc.tenant_type_id = dt.tenant_type_id;
