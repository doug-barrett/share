-- =============================================================================
-- View  : vw_scr_stock_model_calculation_engine_output
-- Schema: {{ catalog }}.{{ silver_schema }}
--
-- Latest version of each KPI result per kpi_engine_key and date_key.
-- QUALIFY filters window results inline — no subquery wrapper needed.
--
--   Parameters (injected at render time from EngineConfig):
--   catalog       : main Unity Catalog name
--   silver_schema : silver layer schema name
--   gold_schema   : gold layer schema name
--   year          : calendar year to filter results
-- =============================================================================

CREATE OR REPLACE VIEW {{ catalog }}.{{ gold_schema }}.vw_scr_stock_model_calculation_engine_output AS
SELECT
    kpi_engine_key,
    matrix_id,
    date_key,
    fiscal_year,
    fiscal_month,
    kpi_id,
    kpi_dim_id,
    version_id,
    value AS kpi_value
FROM {{ catalog }}.{{ silver_schema }}.scr_calculation_engine_output
WHERE fiscal_year = (select max(fiscal_year) from {{ catalog }}.{{ silver_schema }}.scr_calculation_engine_output) 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY kpi_engine_key, date_key
    ORDER BY version_id DESC
) = 1
