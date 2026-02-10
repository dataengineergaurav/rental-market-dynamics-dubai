-- =========================
-- dim_project
-- =========================
with dim_project as (
    select distinct
        project_number,
        project_name_ar,
        project_name_en,
        master_project_ar,
        master_project_en
    from rent_contracts_df
)
select * from dim_project;
