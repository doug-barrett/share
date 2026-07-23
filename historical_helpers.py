-- =============================================================================
-- View  : vw_scr_report_snapshot
-- Schema: {{ catalog }}.{{ gold_schema }}
--
--   Parameters (injected at render time from EngineConfig):
--   catalog          : main Unity Catalog name
--   silver_schema    : silver layer schema name
--   gold_schema      : gold layer schema name
--   reference_schema : reference data schema name
--   aaa_catalog      : AAA catalog name
--   aaa_schema       : AAA schema name
-- =============================================================================

CREATE OR REPLACE VIEW {{ catalog }}.{{ gold_schema }}.vw_scr_report_snapshot AS
(
  WITH calc_engine_actuals AS (
    -- Extracted the calculation engine output into a separate, reusable CTE.
    -- This allows us to safely reference these actuals inside the forecast override logic.
    SELECT
      kpi_engine_key AS kpi_unique_id,
      matrix_id      AS kpi_matrix_id,
      date_key,
      fiscal_year,
      fiscal_month,
      'Actual'       AS scenario,
      kpi_id,
      kpi_dim_id,
      category,
      subcategory,
      kpi_name,
      dim_name,
      uom,
      value
    FROM
      {{ catalog }}.{{ silver_schema }}.scr_calculation_engine_output
    WHERE
      (date_key, version_id) IN (
        SELECT
          date_key,
          MAX(version_id) AS max_version_id
        FROM
          {{ catalog }}.{{ silver_schema }}.scr_calculation_engine_output
        GROUP BY
          date_key
      )
  ),

  scr_kpi AS (
    -- 1. Historic Actuals
    SELECT
      kpi_unique_id AS kpi_unique_id,
      metric_id     AS kpi_matrix_id,
      date_key,
      fiscal_year,
      fiscal_month,
      'Actual'      AS scenario,
      kpi_id,
      kpi_dim_id,
      category,
      subcategory,
      kpi_name,
      dim_name      AS dim_name,
      uom,
      value
    FROM
      {{ catalog }}.{{ silver_schema }}.scr_actuals_historic

    UNION

    -- 2. Budget & Forecast Data
    SELECT
      bf.kpi_unique_id AS kpi_unique_id,
      bf.metric_id     AS kpi_matrix_id,
      bf.date_key,
      bf.fiscal_year,
      bf.fiscal_month,
      CASE
        WHEN bf.scenario = 'Budgets' THEN 'Budget'
        ELSE bf.scenario
      END              AS scenario,
      bf.kpi_id,
      bf.kpi_dim_id,
      bf.category,
      bf.subcategory,
      bf.kpi_name,
      bf.dim_name,
      bf.uom,
      -- If the scenario is 'Forecast' and the record falls in the current month or earlier,
      -- pull the value from calc engine actuals; COALESCE ensures a safe fallback.
      CASE
        WHEN
          LOWER(bf.scenario) = 'forecast'
          AND DATE_TRUNC('month', CAST(bf.date_key AS DATE)) <= DATE_TRUNC('month', CURRENT_DATE())
        THEN
          COALESCE(cea.value, bf.value)
        ELSE bf.value
      END              AS value
    FROM
      {{ catalog }}.{{ silver_schema }}.scr_budget_forecast bf
        LEFT JOIN calc_engine_actuals cea
          ON  bf.kpi_unique_id = cea.kpi_unique_id
          AND bf.date_key      = cea.date_key
        WHERE version_id=(select max(version_id) from {{ catalog }}.{{ silver_schema }}.scr_budget_forecast)
    UNION

    -- 3. Calculation Engine Output (from CTE)
    SELECT
      kpi_unique_id,
      kpi_matrix_id,
      date_key,
      fiscal_year,
      fiscal_month,
      scenario,
      kpi_id,
      kpi_dim_id,
      category,
      subcategory,
      kpi_name,
      dim_name,
      uom,
      value
    FROM
      calc_engine_actuals
  ),

  aaa_adjustments AS (
    SELECT
      sva.kpi_engine_key,
      sva.adj_id,
      sva.adjustment_id,
      sva.adjusted_at,
      sva.month_date,
      sva.original_value,
      sva.adjusted_value,
      sva.adjustment_reason,
      sva.scenario
    FROM
      {{ aaa_catalog }}.{{ aaa_schema }}.scr_value_adjustment sva
  ),

  aaa_variance AS (
    SELECT
      variance.kpi_engine_key,
      variance.variance_id,
      variance.kpi_ref,
      variance.site_code,
      variance.month_date,
      variance.year,
      variance.month,
      variance.variance_commentary,
      'Actual' AS scenario
    FROM
      {{ aaa_catalog }}.{{ aaa_schema }}.scr_variance variance
  )

  -- FINAL SELECT (unchanged to guarantee Power BI dataset integrity)
  SELECT
    kpi.kpi_unique_id,
    kpi.kpi_id,
    kpi.kpi_name,
    kpi.category,
    kpi.subcategory,
    kpi.uom,
    kpi.scenario,
    CAST(kpi.date_key AS DATE)             AS date_key,
    CAST(kpi.value AS DECIMAL(36, 12))     AS kpi_value,
    aaa_adjustments.original_value         AS aaa_original_value,
    aaa_adjustments.adjusted_value         AS aaa_adjusted_value,
    CASE
      WHEN COALESCE(aaa_adjustments.adjusted_value, 0) = 0
      THEN COALESCE(aaa_adjustments.original_value, 0)
      ELSE
        COALESCE(aaa_adjustments.adjusted_value, 0)
        - COALESCE(aaa_adjustments.original_value, 0)
    END                                    AS aaa_adjustment_delta,
    CAST(kpi.value AS DECIMAL(36, 12))     AS current_amount,
    aaa_adjustments.adjustment_reason,
    kpi.kpi_dim_id                         AS location_column_id,
    lt.kpi_dim_name                           AS location_display_name,
    lt.Level                               AS Location_Level,
    lt.Company_Code                        AS Company_code,
    lt.Company_Name                        AS company_name,
    lt.site_code                           AS Site_code,
    lt.site_name                           AS Site_name,
    lt.Type                                AS location_type,
    lt.Type                                AS mine_type,
    lt.company_split                       AS Company_split,
    d.fiscal_year,
    d.fiscal_month                         AS fiscal_period,
    d.fiscal_quarter,
    CONCAT('FY', d.fiscal_year)            AS fiscal_year_label,
    d.month_name,
    v.variance_commentary,
    'kpi_engine'                           AS data_source,
    kpi.kpi_matrix_id,
    kpi.kpi_dim_id,
    CASE
    WHEN DENSE_RANK() OVER (
      PARTITION BY kpi.kpi_id, CAST(kpi.date_key AS DATE), kpi.scenario
      ORDER BY array_position(array('location', 'company', 'site'), LOWER(lt.Level)) NULLS LAST
    ) = 1 THEN 1
    ELSE 0
  END AS location_level_flag,
    kpi.dim_name
  FROM
    scr_kpi AS kpi
      LEFT JOIN (
        SELECT *
        FROM {{ catalog }}.{{ reference_schema }}.scr_location_hierarchy
      ) lt
        ON kpi.kpi_dim_id = lt.kpi_dim_id
      LEFT JOIN (
        SELECT DISTINCT
          date,
          fiscal_year,
          fiscal_month,
          fiscal_quarter,
          month_name
        FROM {{ catalog }}.{{ gold_schema }}.dim_date
      ) d
        ON kpi.date_key = d.date
      LEFT JOIN aaa_adjustments
        ON  kpi.kpi_unique_id   = aaa_adjustments.kpi_engine_key
        AND kpi.date_key        = aaa_adjustments.month_date
        AND UPPER(kpi.scenario) = aaa_adjustments.scenario
      LEFT JOIN aaa_variance v
        ON  kpi.kpi_unique_id   = v.kpi_engine_key
        AND kpi.date_key = v.month_date
        AND kpi.scenario = v.scenario
)
