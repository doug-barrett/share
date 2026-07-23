-- =============================================================================
-- View  : vw_scr_kpi_master
-- Schema: {{ catalog }}.{{ gold_schema }}
-- records flagged as active in the source load. 
--   Parameters (injected at render time from EngineConfig):
--   catalog          : main Unity Catalog name
--   gold_schema      : gold layer schema name
--   reference_schema : reference data schema name
-- =============================================================================

CREATE OR REPLACE VIEW {{ catalog }}.{{ gold_schema }}.vw_scr_kpi_master AS
SELECT *
FROM   {{ catalog }}.{{ reference_schema }}.scr_mgo_kpi_master
WHERE  active_ind = 1
