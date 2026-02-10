-- =========================
-- dim_project
-- =========================
INSERT INTO dim_project (
    project_key,
    project_number,
    project_name_ar,
    project_name_en,
    master_project_ar,
    master_project_en
)
SELECT 
    ROW_NUMBER() OVER () as project_key,
    project_number,
    project_name_ar,
    project_name_en,
    master_project_ar,
    master_project_en
FROM (
    SELECT DISTINCT
        project_number,
        project_name_ar,
        project_name_en,
        master_project_ar,
        master_project_en
    FROM rent_contracts
);
