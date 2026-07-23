-- =============================================================================
-- View  : vw_scr_calculation_engine_output_current_enriched
-- Schema: {{ catalog }}.{{ silver_schema }}
--
--   Parameters (injected at render time from EngineConfig):
--   catalog          : main Unity Catalog name
--   silver_schema    : silver layer schema name
--   reference_schema : reference data schema name
-- =============================================================================

CREATE OR REPLACE VIEW {{ catalog }}.{{ silver_schema }}.vw_scr_calculation_engine_output_current_enriched AS
SELECT
  sceoc.*,
  lt.Location_Level,
  lt.Company_code,
  lt.Company_split,
  lt.Site_code,
  lt.Site_name,
  lt.Mine_site_type
FROM
  {{ catalog }}.{{ silver_schema }}.scr_calculation_engine_output_current sceoc
  LEFT JOIN (
    SELECT
      Level         AS Location_Level,
      Company_Code  AS Company_code,
      company_split AS Company_split,
      site_code     AS Site_code,
      site_name     AS Site_name,
      kpi_dim_id,
      Type          AS Mine_site_type
    FROM
      {{ catalog }}.{{ reference_schema }}.scr_location_hierarchy
  ) lt
    ON sceoc.kpi_dim_id = lt.kpi_dim_id
