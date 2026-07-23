# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # KPI Calculation Engine
# MAGIC
# MAGIC **Entry Point Notebook** — Orchestrates the full SCR KPI calculation pipeline.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC | Layer | Detail |
# MAGIC | --- | --- |
# MAGIC | Config | `env/*.env` files (`dev`, `sit`, `uat`, `prod`). Selected via `env` widget/job parameter. |
# MAGIC | Dependencies | `requirements.txt` installed via `%pip` (cell 2). |
# MAGIC | Fiscal Period | Auto-derived from `year`/`month` params using July–June calendar. |
# MAGIC | Data Loading | `load_all_inputs()` → `(inputs_df, kpi_master_df, kpi_ref_df, kpi_adjustments)` |
# MAGIC | KPI Master | Loaded from Delta `scr_mgo_kpi_master` filtered by `active_ind = 1` (SCD2). |
# MAGIC | Engine | Topological eval of formulas; results written to Delta + Azure SQL. |
# MAGIC
# MAGIC ## Pipeline Steps
# MAGIC
# MAGIC 1. Install dependencies (`requirements.txt`)
# MAGIC 2. Setup sys.path and imports
# MAGIC 3. Initialize configuration (env-driven)
# MAGIC 4. Load all input data (unified inputs, reconcilor, manual, historical, TB adjustments, KPI adjustments)
# MAGIC 5. Build `kpi_ref_dict` and write to Delta
# MAGIC 6. Run calculation engine
# MAGIC 7. Write current results to Delta (overwrite)
# MAGIC 8. Append versioned history to Delta
# MAGIC 9. Log missing dependencies
# MAGIC 10. Build + MERGE to Azure SQL
# MAGIC
# MAGIC >

# COMMAND ----------

# DBTITLE 1,Install dependencies (only if missing)
# MAGIC %pip install -r ./requirements.txt --quiet
# MAGIC
# MAGIC # ============================================================
# MAGIC # All imports (consolidated, deduplicated)
# MAGIC # ============================================================
# MAGIC
# MAGIC # Standard library
# MAGIC import sys
# MAGIC import os
# MAGIC import time
# MAGIC import math
# MAGIC import importlib
# MAGIC from datetime import datetime
# MAGIC
# MAGIC # Third-party
# MAGIC import numpy as np
# MAGIC import pandas as pd
# MAGIC import openpyxl
# MAGIC import pytds
# MAGIC import certifi
# MAGIC import OpenSSL
# MAGIC import azure.identity
# MAGIC from azure.identity import ClientSecretCredential
# MAGIC
# MAGIC # PySpark
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.types import (
# MAGIC     StringType, StructType, StructField, DoubleType,
# MAGIC     IntegerType, TimestampType, DecimalType
# MAGIC )
# MAGIC
# MAGIC # Project modules
# MAGIC import src.config
# MAGIC import src.data_loader
# MAGIC import src.utils
# MAGIC import src.engine
# MAGIC from src.config import create_config
# MAGIC from src.data_loader import load_all_inputs
# MAGIC from src.engine import run_calculation_engine
# MAGIC from src.utils import azure_init

# COMMAND ----------

# DBTITLE 1,Step 0 - Read job and run IDs
# Step 0 - Read job_id and run_id from widget params
# On serverless, dbutils.jobs/context.tags()/spark.conf job keys are all blocked.
# Read job_id and run_id from widget params (set via job base_parameters):
#   job_id: {{job.id}}, run_id: {{job.run_id}}
job_id = None
run_id = None
try:
    params = dbutils.widgets.getAll()
    job_id = params.get("job_id") 
    run_id = params.get("run_id")
except Exception:
    job_id = None
    run_id = None

if job_id is not None and run_id is not None:
    print(f"Job ID: {job_id}")
    print(f"Job Run ID: {run_id}")
else:
    print("Running interactively (no job context available).")

# COMMAND ----------

# DBTITLE 1,Setup sys.path and imports
# Add project root to path and import modules

notebook_path = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
project_root = f"/Workspace{notebook_path}"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Project root: {project_root}")

# COMMAND ----------

# DBTITLE 1,Initialize configuration
# Initialize configuration
importlib.reload(src.config)
from src.config import create_config
# Get env from job base_parameters; default to 'dev' for interactive runs

try:
    params = dbutils.widgets.getAll()
except Exception:
    params = {}

params.setdefault("env", "dev")
params.setdefault("month", "5")
params.setdefault("year", "2026")
params.setdefault("site_code", "MGO")

env = params["env"]
month = params["month"]
year = params["year"]
site_code = params["site_code"]

config = create_config(params)

print(f"Environment:  {env}")
print(f"Site Code:    {site_code}")
print(f"Catalog:      {config.catalog}")
print(f"Fiscal Year:  {config.fiscal_year}  |  Fiscal Month: {config.fiscal_month}")
print(f"Year:         {config.year}  |  Month: {config.month}")
print(f"\nTables:")
print(f"  KPI Master:     {config.kpi_master_table}")
print(f"  SCR Current:    {config.output_table}")
print(f"  SCR History:    {config.scr_history_table}")
print(f"  Reconcilor:     {config.reconcilor_table}")
print(f"  Manual:         {config.manual_table}")
print(f"  Static:         {config.static_table}")
print(f"  Unmatched:      {config.scr_unmatched_table}")
print(f"  Historical:     {config.historical_table}")
print(f"  KPI Adj:        {config.kpi_adjustment_table}")
print(f"  SCR Adj:        {config.scr_value_adjustment}")

# COMMAND ----------

# DBTITLE 1,Step 1 - Load all input data
# Step 1: Load all input data
# Compatibility: DataFrame.map() requires pandas >= 2.1; patch for older versions

if not hasattr(pd.DataFrame, "map"):
    pd.DataFrame.map = pd.DataFrame.applymap

# Reload data_loader to pick up any fixes

importlib.reload(src.utils)
importlib.reload(src.data_loader)
from src.data_loader import load_all_inputs

inputs_df, kpi_master_df, kpi_ref_dict, kpi_adjustments = load_all_inputs(spark, config)

print(f"\nInputs shape: {inputs_df.shape}")
print(f"KPI Master shape: {kpi_master_df.shape}")
print(f"KPI Ref entries: {len(kpi_ref_dict)}")
print(f"KPI Adjustments count: {len(kpi_adjustments)}")


# COMMAND ----------

# DBTITLE 1,Step 3 - Run calculation engine
# Step 3: Run the calculation engine
importlib.reload(src.utils)
importlib.reload(src.engine)
from src.engine import run_calculation_engine

# Patch eval_formula to guard against inf/nan results
def _safe_eval_formula(formula: str) -> float:
    """Safely evaluate a resolved formula string, returning 0 for non-finite results."""
    try:
        result = float(eval(formula))
        return result if np.isfinite(result) else 0.0
    except Exception:
        return 0.0

src.engine.eval_formula = _safe_eval_formula

# Ensure kpi_ref_dict has required 'name' column for engine
if isinstance(kpi_ref_dict, pd.DataFrame) and 'name' not in kpi_ref_dict.columns:
    kpi_ref_dict['name'] = kpi_ref_dict['kpi_dim_name'].astype(str)


results_df, missing_log_df = run_calculation_engine(
    kpi_master_df=kpi_master_df,
    inputs_df=inputs_df,
    kpi_ref_dict=kpi_ref_dict,
    kpi_adjustments=kpi_adjustments,
    fiscal_year=config.fiscal_year,
    fiscal_month=config.fiscal_month,
    year=config.year,
    month=config.month
)

print(f"\nResults preview:")
print(f"date_key type: {type(results_df['date_key'].iloc[0])}")
print(f"date_key value: {results_df['date_key'].iloc[0]}")
#display(results_df)
scr_calculation_engine_history_df = results_df
scr_calculation_engine_current_df = results_df

# COMMAND ----------

# DBTITLE 1,Step 4 - Write current results to Delta
# Step 4: Write results to Delta table

output_table = config.output_table

spark.createDataFrame(scr_calculation_engine_current_df) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(output_table)

print(f"Results written to: {output_table}")
print(f"Total rows: {len(results_df)}")

# COMMAND ----------

# DBTITLE 1,Step 5 - Append versioned history
# Step 5: Append versioned results to Delta history table

scr_history_table = config.scr_history_table
value_type = DecimalType(36, 12)

# Determine next version_id
if spark.catalog.tableExists(scr_history_table):
    # Get the latest version_id for the current date_key
    current_date_key = scr_calculation_engine_history_df["date_key"].iloc[0]
    max_version = spark.sql(
        f"SELECT COALESCE(MAX(version_id), 0) AS max_v FROM {scr_history_table} WHERE date_key = '{current_date_key}'"
    ).collect()[0]["max_v"]
    version_id = max_version + 1
else:
    version_id = 1

# Add version_id to results
scr_calculation_engine_history_df["version_id"] = version_id
#display(scr_calculation_engine_history_df)
# Append to table (preserves all previous versions)
# Cast 'value' to string first to avoid Spark schema inference conflicts
# (pandas object dtype with mixed Decimal/string values causes DELTA_FAILED_TO_MERGE_FIELDS)
history_write_df = scr_calculation_engine_history_df.copy()
history_write_df["value"] = history_write_df["value"].astype(str)

new_data_df = spark.createDataFrame(history_write_df) \
    .withColumn("value", F.col("value").cast(value_type))

# If table exists with narrower decimal precision, widen by rewriting with union
if spark.catalog.tableExists(scr_history_table):
    existing_df = spark.table(scr_history_table) \
        .withColumn("value", F.col("value").cast(value_type))
    existing_df.unionByName(new_data_df, allowMissingColumns=True) \
        .write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(scr_history_table)
else:
    new_data_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(scr_history_table)

print(f"Results written to: {scr_history_table}")
print(f"  Version: {version_id}")
print(f"  Rows appended: {len(results_df)}")

# COMMAND ----------

# DBTITLE 1,Step 6 - Log missing dependencies
# Step 6: Log missing dependencies

if not missing_log_df.empty:
    print(f"Missing dependencies ({len(missing_log_df)} total):")
    display(missing_log_df.head(20))
else:
    print("No missing dependencies - all formulas resolved successfully.")

# COMMAND ----------

# DBTITLE 1,Init — Dependencies and Config
# ============================================================
# Cell 1 — Init: Dependencies, Config, Helpers
# ============================================================

# --- NotebookUtils (reusable across notebooks) ---
importlib.reload(src.utils)
from src.utils import azure_init

nb = azure_init(config, spark)
nb.print_config()
nb.verify_tables()

# --- Backward-compatible aliases (used by downstream cells) ---
reporting_month   = nb.reporting_month
scenario          = nb.scenario
AZSQL_SCHEMA_NAME = nb.azsql_schema
start_timer       = nb.start_timer
end_timer         = nb.end_timer

print("✓ Init complete")

# COMMAND ----------

# DBTITLE 1,Step 7 - Build and MERGE to Azure SQL

# ============================================================
# Step 7 — Build + MERGE to Azure SQL
# Transformation via SQL CTEs; MERGE push via pytds.
# ============================================================
start_timer('build_and_push_sql')

# ── TEST FILTER ──────────────────────────────────────────────

#job_id = '130350402661888'
#run_id = '888234517065852'
# ── HELPERS (unchanged) ──────────────────────────────────────
# Delegate to NotebookUtils static helpers (defined in src/notebook_utils.py)
_sanitise = nb.sanitise
_sql_type  = nb.sql_type

print("=" * 80)
print(f"  BUILD + MERGE to Azure SQL [{AZSQL_SCHEMA_NAME}]")
print("=" * 80)

try:
    _id_year = reporting_month[:4]

    # ── 1. UOM lookup (SQL) ──────────────────────────────────
    _uom_sql = f"""
    WITH uom_raw AS (
        SELECT kpi_id, uom FROM {config.actuals_historic_table} WHERE uom IS NOT NULL AND uom NOT IN ('UOM', 'UoM', '0')
        UNION
        SELECT kpi_id, uom FROM {config.budget_forecast_table}  WHERE uom IS NOT NULL AND uom NOT IN ('UOM', 'UoM', '0')
        UNION
        SELECT kpi_id, uom FROM {config.output_table}           WHERE uom IS NOT NULL AND uom NOT IN ('UOM', 'UoM', '0')
    ),
    uom_ranked AS (
        SELECT kpi_id, uom,
               ROW_NUMBER() OVER (PARTITION BY kpi_id ORDER BY uom) AS _rn
        FROM uom_raw
    )
    SELECT kpi_id AS _uid, uom AS _uom
    FROM uom_ranked
    WHERE _rn = 1
    """
    _uom = spark.sql(_uom_sql)
    _uom.createOrReplaceTempView("_v_uom")

    # ── 2. Enrichment lookup ─────────────────────────────────
    _enrich_sql = f"""
    SELECT kpi_engine_key AS _ek,
           Site_code      AS _site,
           matrix_id as _matrix_id,
           Company_code   AS _company
    FROM {config.enriched_view}
    """
    _enrich = spark.sql(_enrich_sql)
    _enrich.createOrReplaceTempView("_v_enrich")

    _slt_team_sql = f"""
    SELECT kpi_matrix_id AS _matrix_id,
           team_name      AS _team_name
    FROM {config.catalog}.{config.reference_schema}.kpi_slt_team
    """
    _slt_team = spark.sql(_slt_team_sql)
    _slt_team.createOrReplaceTempView("_v_slt_team")

    # ── 3. scr_value_adjustment (SQL) — TEST FILTERED ───────
    sva_sql = f"""
    WITH base AS (
        SELECT
            kpi_engine_key,
            kpi_id                          AS kpi_reference,
            CAST(kpi_dim_id AS INT)         AS location_column_id,
            '{reporting_month}'             AS month_date,
            '{scenario}'                    AS scenario,
            value                           AS original_value,
            CAST(NULL AS DOUBLE)            AS adjusted_value,
            CAST(NULL AS STRING)            AS adjustment_reason,
            CAST(NULL AS STRING)            AS adjusted_by,
            CAST(NULL AS TIMESTAMP)         AS adjusted_at,
            'DRAFT'                         AS status,
            CAST(NULL AS STRING)            AS approved_by,
            CAST(NULL AS TIMESTAMP)         AS approved_at,
            coalesce(nullif(scr_master.scr_category, ''),calc_engine.category) AS kpi_category,
            coalesce(nullif(scr_master.scr_subcategory,''), calc_engine.subcategory) AS kpi_subcategory,
            coalesce(nullif(scr_master.scr_kpi_name,''), calc_engine.kpi_name) AS kpi_aggregation,
            dim_name                        AS location_name,
            YEAR('{reporting_month}')       AS year,
            MONTH('{reporting_month}')      AS month,
            CAST(NULL AS INT)               AS priority,
            CASE WHEN source = 'Calculated from other KPIs' THEN 'CALCULATED KPI'
                 WHEN source IS NULL THEN 'N/A'
                 ELSE 'BASE KPI'
            END AS source
        FROM {config.output_table} as calc_engine
        LEFT JOIN {config.catalog}.{config.reference_schema}.kpi_mgo_scr_master as scr_master
        ON (calc_engine.matrix_id=scr_master.matrix_id)
        
    ),
    numbered AS (
        SELECT *,
               ROW_NUMBER() OVER (ORDER BY kpi_reference, location_column_id) AS _rn
        FROM base
    )
    SELECT
        n.kpi_engine_key,
        n.kpi_reference,
        n.location_column_id,
        n.month_date,
        n.scenario,
        n.original_value,
        n.adjusted_value,
        n.adjustment_reason,
        n.adjusted_by,
        n.adjusted_at,
        n.status,
        n.approved_by,
        n.approved_at,
        n.kpi_category,
        n.kpi_subcategory,
        n.kpi_aggregation,
        n.location_name,
        n.year,
        n.month,
        n.priority,
        CAST(n._rn AS INT)                                              AS adj_id,
        CONCAT('ADJ-{_id_year}-', LPAD(CAST(n._rn AS STRING), 4, '0')) AS adjustment_id,
        e._site                                                         AS site_code,
        u._uom                                                          AS unit_of_measure,
        n.source
    FROM numbered n
    LEFT JOIN _v_enrich e ON n.kpi_engine_key = e._ek
    LEFT JOIN _v_uom    u ON n.kpi_reference  = u._uid
    """
    sva_df = spark.sql(sva_sql)
    print(f"  ✓ scr_value_adjustment: {sva_df.count():,} rows")
    #display(sva_df)

    # ── 4. scr_variance — current month (SQL) — TEST FILTERED ─
    sv_current_sql = f"""
    WITH bf_current AS (
        SELECT *
        FROM {config.budget_forecast_table}
        WHERE date_key = '{reporting_month}'
          AND kpi_dim_id RLIKE '^[0-9]+$'
    ),
    budget_ranked AS (
        SELECT kpi_id, kpi_dim_id, value AS budget_value,
               ROW_NUMBER() OVER (PARTITION BY kpi_id, kpi_dim_id ORDER BY version_id DESC) AS _rn
        FROM bf_current
        WHERE scenario = 'Budgets'
    ),
    budget AS (
        SELECT kpi_id AS b_kid, kpi_dim_id AS b_did, budget_value
        FROM budget_ranked WHERE _rn = 1
    ),
    forecast_ranked AS (
        SELECT kpi_id, kpi_dim_id, value AS forecast_value,
               ROW_NUMBER() OVER (PARTITION BY kpi_id, kpi_dim_id ORDER BY version_id DESC) AS _rn
        FROM bf_current
        WHERE scenario = 'Forecast'
    ),
    forecast AS (
        SELECT kpi_id AS f_kid, kpi_dim_id AS f_did, forecast_value
        FROM forecast_ranked WHERE _rn = 1
    )
    SELECT
        k.kpi_engine_key,
        k.kpi_id                            AS kpi_ref,
        k.kpi_name                          AS kpi_aggregation,
        k.category                          AS kpi_category,
        k.subcategory                       AS kpi_subcategory,
        CAST(NULL AS STRING)                AS unit_of_measure,
        k.dim_name                          AS location_name,
        CAST(NULL AS STRING)                AS site_code,
        CAST(NULL AS STRING)                AS company_code,
        '{reporting_month}'                 AS month_date,
        YEAR('{reporting_month}')           AS year,
        MONTH('{reporting_month}')          AS month,
        CAST(k.execution_level AS BIGINT)   AS computation_level,
        k.value                             AS actual_value,
        b.budget_value,
        f.forecast_value,
        k.value - b.budget_value            AS budget_variance,
        CASE WHEN b.budget_value != 0
             THEN (k.value - b.budget_value) / b.budget_value * 100
             ELSE NULL END                  AS budget_variance_pct,
        k.value - f.forecast_value          AS forecast_variance,
        CASE WHEN f.forecast_value != 0
             THEN (k.value - f.forecast_value) / f.forecast_value * 100
             ELSE NULL END                  AS forecast_variance_pct,
        CAST(NULL AS STRING)                AS variance_commentary,
        CAST(NULL AS INT)                   AS var_id,
        CAST(NULL AS INT)                   AS priority,
        CASE WHEN source = 'Calculated from other KPIs' THEN 'CALCULATED KPI'
             WHEN source IS NULL THEN 'N/A'
             ELSE 'BASE KPI'
        END AS source
    FROM {config.output_table} k
    LEFT JOIN budget   b ON k.kpi_id = b.b_kid AND CAST(k.kpi_dim_id AS STRING) = b.b_did
    LEFT JOIN forecast f ON k.kpi_id = f.f_kid AND CAST(k.kpi_dim_id AS STRING) = f.f_did
   
    """
    spark.sql(sv_current_sql).createOrReplaceTempView("_v_sv_current")

    # ── 5. scr_variance — historical months (SQL) — TEST FILTERED
    print({config.output_table})
    key_lookup = f"""
        SELECT DISTINCT concat(kpi_id,'_',kpi_dim_id) as kpi_matrix_id, kpi_id, kpi_dim_id, execution_level,
               dim_name, category, subcategory, kpi_name
        FROM {config.output_table}
    """
    #display(spark.sql(key_lookup))
    spark.sql(key_lookup).createOrReplaceTempView("key_lookup")

    hist_bf_pivot = f"""
    SELECT
      concat(kpi_id,'_',kpi_dim_id) as kpi_matrix_id,
      kpi_id,
      kpi_dim_id,
      date_key,
      kpi_unique_id as kpi_engine_key,
      MAX(version_id) AS latest_version_id,
      MAX(CASE WHEN scenario = 'Budgets' THEN value END) AS budget_value,
      MAX(CASE WHEN scenario = 'Forecast' THEN value END) AS forecast_value
    FROM {config.budget_forecast_table}
    WHERE date_key != '{reporting_month}'
      AND kpi_dim_id RLIKE '^[0-9]+$'
    GROUP BY all
    """
    #display(spark.sql(hist_bf_pivot))
    spark.sql(hist_bf_pivot).createOrReplaceTempView("hist_bf_pivot")

    hist_actuals = f""" SELECT metric_id,value,date_key,fiscal_year,fiscal_month,scenario,source_file,kpi_id,kpi_dim_id,dim_name,kpi_name,category,subcategory,uom,kpi_unique_id as kpi_engine_key,CAST(kpi_dim_id AS BIGINT) AS kpi_dim_id_long
        FROM {config.actuals_historic_table}
        WHERE CAST(kpi_dim_id AS BIGINT) IS NOT NULL """

    #display(spark.sql(hist_actuals))
    spark.sql(hist_actuals).createOrReplaceTempView("hist_actuals")

    sv_hist_sql =f"""SELECT
        coalesce(ha.kpi_engine_key, hbf.kpi_engine_key) as kpi_engine_key,
        ha.kpi_id                           AS kpi_ref,
        kl.kpi_name                         AS kpi_aggregation,
        kl.category                         AS kpi_category,
        kl.subcategory                      AS kpi_subcategory,
        ha.uom                              AS unit_of_measure,
        kl.dim_name                         AS location_name,
        CAST(NULL AS STRING)                AS site_code,
        CAST(NULL AS STRING)                AS company_code,
        ha.date_key                         AS month_date,
        YEAR(ha.date_key)                   AS year,
        MONTH(ha.date_key)                  AS month,
        CAST(kl.execution_level AS BIGINT)  AS computation_level,
        ha.value                            AS actual_value,
        hbf.budget_value,
        hbf.forecast_value,
        ha.value - hbf.budget_value         AS budget_variance,
        CASE WHEN hbf.budget_value != 0
             THEN (ha.value - hbf.budget_value) / hbf.budget_value * 100
             ELSE NULL END                  AS budget_variance_pct,
        ha.value - hbf.forecast_value       AS forecast_variance,
        CASE WHEN hbf.forecast_value != 0
             THEN (ha.value - hbf.forecast_value) / hbf.forecast_value * 100
             ELSE NULL END                  AS forecast_variance_pct,
        CAST(NULL AS STRING)                AS variance_commentary,
        CAST(NULL AS INT)                   AS var_id,
        CAST(NULL AS INT)                   AS priority,
        CAST(NULL AS STRING)                AS SOURCE
    FROM hist_actuals ha
    INNER JOIN key_lookup kl ON concat(ha.kpi_id,'_',ha.kpi_dim_id) = kl.kpi_matrix_id
    LEFT JOIN hist_bf_pivot hbf
        ON concat(ha.kpi_id,'_',ha.kpi_dim_id) = hbf.kpi_matrix_id
       AND ha.date_key = hbf.date_key
    /*WHERE ha.kpi_id = 'K4'*/"""

    #display(spark.sql(sv_hist_sql))
    spark.sql(sv_hist_sql).createOrReplaceTempView("_v_sv_hist")

    # ── 6. Union + enrich scr_variance (SQL) ─────────────────
    sv_final_sql = f"""
    WITH combined AS (
        SELECT * FROM _v_sv_current
        UNION ALL
        SELECT * FROM _v_sv_hist
    ),
    numbered AS (
        SELECT *,
               ROW_NUMBER() OVER (ORDER BY kpi_ref, month_date, location_name) AS _rn
        FROM combined
    )
    SELECT
        n.kpi_engine_key,
        n.kpi_ref,
        n.kpi_aggregation,
        n.kpi_category,
        n.kpi_subcategory,
        COALESCE(n.unit_of_measure, u._uom) AS unit_of_measure,
        n.location_name,
        e._site                             AS site_code,
        COALESCE(e._company, n.company_code) AS company_code,
        n.month_date,
        n.year,
        n.month,
        n.computation_level,
        n.actual_value,
        n.budget_value,
        n.forecast_value,
        n.budget_variance,
        n.budget_variance_pct,
        n.forecast_variance,
        n.forecast_variance_pct,
        n.variance_commentary,
        CAST(n._rn AS INT)                                              AS var_id,
        n.priority,
        CONCAT('VAR-{_id_year}-', LPAD(CAST(n._rn AS STRING), 5, '0')) AS variance_id,
        CASE WHEN t._team_name  is null then 'Unknown' else t._team_name end as site_leadership_team
    FROM numbered n
    LEFT JOIN _v_enrich e ON n.kpi_engine_key = e._ek
    LEFT JOIN _v_uom    u ON n.kpi_ref        = u._uid
    LEFT JOIN _v_slt_team t on t._matrix_id =e._matrix_id
    """
    sv_df = spark.sql(sv_final_sql)
    print(f"  ✓ scr_variance: {sv_df.count():,} rows")
    display(sv_df)

    # ── 7. Empty audit + annotation frames ───────────────────
    audit_df = spark.createDataFrame([], StructType([
        StructField("audit_id", StringType()), StructField("event_type", StringType()),
        StructField("event_source", StringType()), StructField("kpi_reference", StringType()),
        StructField("location_column_id", IntegerType()), StructField("month_date", StringType()),
        StructField("scenario", StringType()), StructField("old_value", DoubleType()),
        StructField("new_value", DoubleType()), StructField("performed_by", StringType()),
        StructField("performed_at", TimestampType()), StructField("details", StringType()),
        StructField("modify", TimestampType()), StructField("status", StringType()),
        StructField("variance_commentary", StringType()), StructField("rationale", StringType()),
        StructField("location", StringType()), StructField("change_id", IntegerType()),
        StructField("metric", StringType()), StructField("modified_by", StringType()),
        StructField("comments", StringType())
    ]))
    annotation_df = spark.createDataFrame([], StructType([
        StructField("annotation_id", StringType()), StructField("kpi_reference", StringType()),
        StructField("location_column_id", IntegerType()), StructField("month_date", StringType()),
        StructField("scenario", StringType()), StructField("commentary", StringType()),
        StructField("team", StringType()), StructField("created_by", StringType()),
        StructField("created_at", TimestampType()), StructField("is_deleted", StringType()),
        StructField("role", StringType()), StructField("modified", TimestampType()),
        StructField("ann_id", IntegerType()), StructField("modified_by", StringType())
    ]))
    print("  ✓ scr_audit_trail + scr_annotation (0 rows)")

    # ── 8. MERGE to Azure SQL (with conditional original_value update) ───────
    print("\n" + "=" * 80)
    print(f"  MERGE → Azure SQL [{AZSQL_SCHEMA_NAME}]")
    print("=" * 80)

    _cred = ClientSecretCredential(
        config.azsql_tenant_id,
        config.azsql_client_id,
        dbutils.secrets.get(config.azsql_secret_scope, config.azsql_secret_key)
    )
    _conn = pytds.connect(
        dsn=config.azsql_server, database=config.azsql_database,
        port=1433, access_token_callable=lambda: _cred.get_token("https://database.windows.net/.default").token,
        cafile=certifi.where(), autocommit=True
    )
    _cur = _conn.cursor()
    print("  Connected")

    _MERGES = [
        {'df': sva_df, 'table': 'scr_value_adjustment', 'id_col': 'adj_id',
         'merge_keys': ['kpi_engine_key', 'month_date', 'scenario'],
         'update_cols': ['adjustment_id', 'kpi_reference', 'location_column_id',
                         'original_value', 'kpi_category', 'kpi_subcategory',
                         'kpi_aggregation', 'unit_of_measure', 'location_name',
                         'site_code', 'year', 'month', 'source']},
        {'df': sv_df, 'table': 'scr_variance', 'id_col': 'var_id',
         'merge_keys': ['kpi_ref', 'location_name', 'month_date'],
         'update_cols': ['variance_id', 'kpi_engine_key', 'kpi_aggregation', 'kpi_category',
                         'kpi_subcategory', 'unit_of_measure',
                         'site_code', 'company_code', 'year', 'month',
                         'computation_level', 'actual_value', 'budget_value',
                         'forecast_value', 'budget_variance', 'budget_variance_pct',
                         'forecast_variance', 'forecast_variance_pct','site_leadership_team']},
        {'df': audit_df, 'table': 'scr_audit_trail', 'id_col': 'change_id', 'merge_keys': None, 'update_cols': None},
        {'df': annotation_df, 'table': 'scr_annotation', 'id_col': 'ann_id', 'merge_keys': None, 'update_cols': None},
    ]

    for m in _MERGES:
        tbl, id_col, keys, ucols = m['table'], m['id_col'], m['merge_keys'], m['update_cols']
        try:
            df = m['df'].dropDuplicates(keys) if keys else m['df']
            pdf = df.toPandas()
            if len(pdf) == 0:
                print(f"  ─ {tbl:30s} (0 rows, skipped)")
                continue
            cols = [c for c in pdf.columns if c != id_col]
            tmp = f"#tmp_{tbl[:20]}"
            _cur.execute(f"IF OBJECT_ID('tempdb..{tmp}') IS NOT NULL DROP TABLE {tmp}")
            # Pre-compute dtypes and positions outside row loop (SCPAP001 fix)
            col_dtypes = {c: _sql_type(pdf[c].dtype) for c in cols}
            col_positions = {c: pdf.columns.get_loc(c) for c in cols}
            col_defs = ', '.join(f'[{c}] {col_dtypes[c]}' for c in cols)
            _cur.execute(f"CREATE TABLE {tmp} ({col_defs})")
            rows = [tuple(_sanitise(row[col_positions[c]]) for c in cols) for row in pdf.itertuples(index=False, name=None)]
            sql_ins = f"INSERT INTO {tmp} ({', '.join(f'[{c}]' for c in cols)}) VALUES ({', '.join(['%s']*len(cols))})"
            for i in range(0, len(rows), 1000):
                _cur.executemany(sql_ins, rows[i:i+1000])
            if keys and ucols:
                # --- Conditional MERGE for scr_value_adjustment ---
                # Uses CASE expression to conditionally preserve original_value:
                # If adjusted_value IS NOT NULL → keep existing original_value
                # If adjusted_value IS NULL     → update original_value from source
                if tbl == 'scr_value_adjustment':
                    # Build SET clause with CASE for original_value
                    set_parts = []
                    for c in ucols:
                        if c == 'original_value':
                            set_parts.append(
                                f"t.[{c}] = CASE WHEN t.[adjusted_value] IS NULL THEN s.[{c}] ELSE t.[{c}] END"
                            )
                        else:
                            set_parts.append(f"t.[{c}] = s.[{c}]")
                    merge_sql = f"""
                        MERGE [{AZSQL_SCHEMA_NAME}].[{tbl}] AS t USING {tmp} AS s
                        ON {' AND '.join(f't.[{k}] = s.[{k}]' for k in keys)}
                        WHEN MATCHED THEN
                            UPDATE SET {', '.join(set_parts)}
                        WHEN NOT MATCHED THEN
                            INSERT ({', '.join(f'[{c}]' for c in cols)})
                            VALUES ({', '.join(f's.[{c}]' for c in cols)});
                    """
                    print(f"    → original_value: CASE-protected (preserves when adjusted_value IS NOT NULL)")
                else:
                    merge_sql = f"""
                        MERGE [{AZSQL_SCHEMA_NAME}].[{tbl}] AS t USING {tmp} AS s
                        ON {' AND '.join(f't.[{k}] = s.[{k}]' for k in keys)}
                        WHEN MATCHED THEN UPDATE SET {', '.join(f't.[{c}] = s.[{c}]' for c in ucols)}
                        WHEN NOT MATCHED THEN INSERT ({', '.join(f'[{c}]' for c in cols)}) VALUES ({', '.join(f's.[{c}]' for c in cols)});
                    """
                _cur.execute(merge_sql)
            else:
                _cur.execute(f"INSERT INTO [{AZSQL_SCHEMA_NAME}].[{tbl}] ({', '.join(f'[{c}]' for c in cols)}) SELECT {', '.join(f'[{c}]' for c in cols)} FROM {tmp}")
            _cur.execute(f"DROP TABLE {tmp}")
            _cur.execute(f"SELECT COUNT(*) FROM [{AZSQL_SCHEMA_NAME}].[{tbl}]")
            print(f"  ✓ {tbl:30s} ({len(pdf):>6,} merged → {_cur.fetchone()[0]:>6,} total)")
        except Exception as e:
            print(f"  ERROR merging {tbl}: {e}")

    print("\n  Validation:")
    try:
        _cur.execute(f"SELECT COUNT(*) FROM [{AZSQL_SCHEMA_NAME}].[scr_value_adjustment] WHERE unit_of_measure IS NOT NULL")
        _uom1 = _cur.fetchone()[0]
        _cur.execute(f"SELECT COUNT(*) FROM [{AZSQL_SCHEMA_NAME}].[scr_value_adjustment]")
        print(f"    UOM (adj): {_uom1}/{_cur.fetchone()[0]}")
        _cur.execute(f"SELECT COUNT(*) FROM [{AZSQL_SCHEMA_NAME}].[scr_variance] WHERE unit_of_measure IS NOT NULL")
        _uom2 = _cur.fetchone()[0]
        _cur.execute(f"SELECT COUNT(*) FROM [{AZSQL_SCHEMA_NAME}].[scr_variance]")
        print(f"    UOM (var): {_uom2}/{_cur.fetchone()[0]}")
        print(f"   Job id: {job_id}")
        print(f"    Run Id: {run_id}")
    except Exception as e:
        print(f"    ERROR validating UOM: {e}")

    print("\n  Cleanup:")
    try:
        # Update job status to Completed if job_id and run_id are not None
        if job_id is not None and run_id is not None:
            _cur.execute(f"""
                UPDATE [edp_aaa].[scr_job_site_mapping]
                SET status = 'Completed',
                    TimeStamp = %s
                WHERE job_code = %s AND run_id = %s
            """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), job_id, run_id))
            print(f"  ✓ Job status updated to Completed for job_code={job_id}, run_id={run_id}")

    except Exception as e:
        print(f"  ERROR during validation: {e}")
        # Update job status to Failed if job_id and run_id are not None
        if job_id is not None and run_id is not None:
            try:
                _cur.execute(f"""
                    UPDATE [edp_aaa].[scr_job_site_mapping]
                    SET status = 'Failed',
                        TimeStamp = %s
                    WHERE job_code = %s AND run_id = %s
                """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), job_id, run_id))
                print(f"  ✓ Job status updated to Failed for job_code={job_id}, run_id={run_id}")
            except Exception as e2:
                print(f"  ERROR updating job status to Failed: {e2}")

    _cur.close()
    _conn.close()

    print("  ✓ Done")
    end_timer('build_and_push_sql')

except Exception as e:
    import traceback
    print("ERROR in Build + MERGE to Azure SQL (SQL-based):")
    print(traceback.format_exc())

# COMMAND ----------

# DBTITLE 1,Pipeline Exit
# Pipeline complete
dbutils.notebook.exit(f"SUCCESS: {len(results_df)} KPIs calculated and written to {config.output_table}")