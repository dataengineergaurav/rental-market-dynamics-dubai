-- =========================
-- fact_rental_contract
-- =========================
WITH today_date AS (
	select full_date from dim_date where full_date = DATE('NOW')
)
, data as (
	select
			DATE(
		    substr(rc.contract_start_date,7,4) || '-' ||
		    substr(rc.contract_start_date,4,2) || '-' ||
		    substr(rc.contract_start_date,1,2)
			) AS contract_start_date,
			DATE(
				    substr(contract_end_date,7,4) || '-' ||
				    substr(contract_end_date,4,2) || '-' ||
				    substr(contract_end_date,1,2)
			) AS contract_end_date,
		  *
	FROM rent_contracts_df rc 
)
, expired_today as (
	select 
	  dct.contract_type_key,
	  dp.property_key,
	  dprj.project_key,
	  dl.location_key, 
	  dt.tenant_key,
	  d.contract_start_date,
	  d.contract_end_date,
	  d.annual_amount,
	  d.contract_amount,
	  d.no_of_prop,
	  d.line_number,
	  d.is_free_hold
	from data d
	JOIN today_date td
	on d.contract_end_date = td.full_date 
	JOIN dim_contract_type dct
	  ON d.contract_reg_type_id = dct.contract_reg_type_id
	JOIN dim_project dprj
	  ON d.project_number = dprj.project_number
	JOIN dim_property dp
	  ON d.ejari_bus_property_type_id  = dp.ejari_bus_property_type_id  
	JOIN dim_location dl
	  ON d.area_id = dl.area_id  
	JOIN dim_tenant dt
	  ON d.tenant_type_id = dt.tenant_type_id  
)
SELECT * from expired_today;
