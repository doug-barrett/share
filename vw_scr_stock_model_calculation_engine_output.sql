"""
Data loading module for the KPI Calculation Engine.

Handles loading data from Unity Catalog Delta tables.
All paths are parameterized via EngineConfig.
"""

import pandas as pd
from pyspark.sql import SparkSession

from .config import EngineConfig
from .utils import clean_input_ref, merge_excel_overlay, check_duplicates
from .utils import _to_decimal
INPUT_COLUMNS = ["input_ref", "source", "fin_period", "fin_year", "value"]


def write_excel_to_delta(spark, excel_path, table_name, config, columns=None):
    """
    Read Excel file, add date_key, clean columns, write to Delta table, and return DataFrame.
    If excel_path is None or the file is not found, prints a skip message and returns an empty DataFrame.
    """
    if excel_path is None:
        print(f"File not found — skipping: {table_name}")
        return pd.DataFrame(columns=columns) if columns else pd.DataFrame()
    try:
        df = pd.read_excel(excel_path, keep_default_na=False)
        if columns is not None:
            df = df[columns]
        df["date_key"] = pd.Timestamp(year=config.year, month=config.month, day=1).date()
        col_names = df.columns.str.strip().str.replace(' ', '_').str.lower()
        df.columns = col_names
        spark.createDataFrame(df) \
            .write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
        return df
    except Exception as e:
        print(f"Error reading file — skipping: {excel_path} ({e})")
        return pd.DataFrame(columns=columns) if columns else pd.DataFrame()


def load_kpi_master(spark: SparkSession, config: EngineConfig) -> tuple:
    """
    Load KPI master and reference data from Unity Catalog.
    Filters master to active records (active_ind = 1) from the SCD2 table.
    Returns (kpi_master_df, kpi_ref_df).
    """
    kpi_master_df = (
        spark.table(config.kpi_master_table)
        .filter("active_ind = 1")
        .toPandas()
    )
    kpi_ref_df = spark.table(config.kpi_ref_table).toPandas()
    return kpi_master_df, kpi_ref_df


def load_location_hierarchy(spark: SparkSession, config: EngineConfig) -> pd.DataFrame:
    """Load location hierarchy data directly from Unity Catalog."""
    return spark.table(config.scr_location_hierarchy_table).toPandas()


def load_unified_inputs(spark: SparkSession, config: EngineConfig) -> pd.DataFrame:
    """Load unified input reference data filtered by fiscal year/month."""
    query = f"""
    SELECT
        input_ref,
        source_name AS source,
        fiscal_month AS fin_period,
        fiscal_year AS fin_year,
        Measure_value AS value
    FROM {config.unified_input_ref_table}
    WHERE fiscal_year = {config.fiscal_year}
      AND fiscal_month = {config.fiscal_month}
    and site_code = '{config.site_code}' 
    """
    #print(query)
    df = spark.sql(query).toPandas()
    df["input_ref"] = clean_input_ref(df["input_ref"])
    check_duplicates(df)
    return df


def load_reconcilor_and_manual(spark: SparkSession, config: EngineConfig) -> pd.DataFrame:
    """
    Load Reconcilor, Manual, Static, and SCR Unmatched data from Excel -> Delta,
    merge them, and standardize columns. Any file not found is skipped.
    """
    sources = [
        ("Reconcilor", config.reconcilor_excel_path, config.reconcilor_table),
        ("Manual", config.manual_excel_path, config.manual_table),
        ("Static", config.scr_static_excel_path, config.static_table),
        ("SCR Unmatched", config.scr_unmatched_excel_path, config.scr_unmatched_table),
    ]

    dfs_to_merge = []
    for label, excel_path, table_name in sources:
        print(f"{label}_excel_path:      {excel_path}")
        df = write_excel_to_delta(spark, excel_path, table_name, config)
        if not df.empty:
            print(f"{label} data written to: {table_name}")
            dfs_to_merge.append(df)

    if not dfs_to_merge:
        return pd.DataFrame(columns=INPUT_COLUMNS)

    merged_df = pd.concat(dfs_to_merge, ignore_index=True)
    # Normalise column names to lowercase so that mixed-case variants across
    # files (e.g. "Source" vs "source", "SCR_Value" vs "scr_value") don't
    # create duplicate columns after concat and rename.
    merged_df.columns = merged_df.columns.str.lower()
    excel_df = merged_df.rename(columns={
        "i_ref_values": "input_ref",
        "scr_value": "value"
    })
    excel_df["fin_period"] = config.fiscal_month
    excel_df["fin_year"] = config.fiscal_year
    excel_df = excel_df[INPUT_COLUMNS]
    excel_df["input_ref"] = clean_input_ref(excel_df["input_ref"])
    return excel_df


def load_historical_data(spark: SparkSession, config: EngineConfig) -> pd.DataFrame:
    """Load historically calculated KPIs from Excel -> Delta."""
    print(f"Historical_excel_path:      {config.historical_excel_path}")
    historical_df = write_excel_to_delta(
        spark, config.historical_excel_path, config.historical_table, config
    )
    if historical_df.empty:
        return pd.DataFrame(columns=INPUT_COLUMNS)
    historical_df["fin_period"] = config.fiscal_month
    historical_df["fin_year"] = config.fiscal_year
    historical_df["source"] = "manual precalculated historic values"
    historical_df = historical_df[INPUT_COLUMNS]
    historical_df["input_ref"] = clean_input_ref(historical_df["input_ref"])
    return historical_df


def load_tb_adjustments(spark: SparkSession, config: EngineConfig) -> pd.DataFrame:
    """
    Load manual TB adjustments and apply to input_to_gl_account_table.
    Returns DataFrame of adjusted input values.
    """
    print(f"TB_Adj_excel_path:      {config.tb_adjustment_excel_path}")
    tb_adj_df = write_excel_to_delta(
        spark, config.tb_adjustment_excel_path, config.tb_adjustment_table, config
    )
    if tb_adj_df.empty:
        return pd.DataFrame(columns=INPUT_COLUMNS)
    if "account_number" not in tb_adj_df.columns:
        return pd.DataFrame(columns=INPUT_COLUMNS)
    tb_adj_df["account_number"] = tb_adj_df["account_number"].astype(str)

    input_df = spark.table(config.input_to_gl_account_table).toPandas()
    input_df = input_df[
        (input_df["fiscal_year"] == config.fiscal_year) &
        (input_df["fiscal_month"] == config.fiscal_month)
    ]
    input_df["account_number"] = input_df["gl_account"].astype(str)
    input_tb_df = input_df.merge(
        tb_adj_df[["account_number", "value"]],
        on=["account_number"],
        how="left"
    ).reset_index(drop=True)
    input_tb_df["adjusted_value"] = input_tb_df["value"].fillna(input_tb_df["measure_value"])
    affected_inputs = input_tb_df.loc[input_tb_df["value"].notna(), "input_ref"].unique()
    input_tb_df = input_tb_df[input_tb_df["input_ref"].isin(affected_inputs)]
    result = (
        input_tb_df.groupby(["input_ref", "fiscal_month", "fiscal_year"], as_index=False)["adjusted_value"]
        .sum()
        .rename(columns={"adjusted_value": "value"})
    )
    result["input_ref"] = clean_input_ref(result["input_ref"])
    result["fin_period"] = config.fiscal_month
    result["fin_year"] = config.fiscal_year
    result["source"] = "trial balance adjustments"

    return result[INPUT_COLUMNS]

def load_tb_adjustments_aaa(spark: SparkSession, config: EngineConfig) -> pd.DataFrame:
    """
    Load manual TB adjustments and apply to input_to_gl_account_table.
    Returns DataFrame of adjusted input values.
    """
    _tb = spark.table(config.scr_trial_balance_table)
    tb_spark_df = (
        _tb.filter((_tb["month"] == config.month) & (_tb["year"] == config.year) & (_tb["site_code"] == config.site_code))
        .selectExpr(
            "account_number",
            "CAST(movement AS DECIMAL(36,12)) AS movement",
            "CAST(adjustment AS DECIMAL(36,12)) AS adjustment",
            "CAST(current_movement AS DECIMAL(36,12)) AS current_movement",
            "site_code",
            "month",
            "year"
        )
    )
#  "CAST(CASE WHEN current_movement IS NULL OR current_movement = 0 THEN movement ELSE current_movement END AS DECIMAL(36,12)) AS current_movement",

    # Write Spark DataFrame directly to Delta — preserves DECIMAL(36,12) types.
    # Converting to pandas first would lose precision (pandas infers float64/object).
    tb_spark_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(config.tb_adjustment_table)

    tb_adj_df = tb_spark_df.toPandas()

    if tb_adj_df.empty:
        return pd.DataFrame(columns=INPUT_COLUMNS)
    if "account_number" not in tb_adj_df.columns:
        return pd.DataFrame(columns=INPUT_COLUMNS)
    tb_adj_df["account_number"] = tb_adj_df["account_number"].astype(str)

    input_df = spark.table(config.input_to_gl_account_table).toPandas()
    input_df = input_df.rename(columns={
        'FiscalYear': 'fiscal_year',
        'FiscalMonth': 'fiscal_month',
        'GlAccountcode': 'gl_account',
        'InputRef': 'input_ref',
        'Value': 'measure_value',
        'SiteCode': 'site_code',
        'Year': 'year',
        'Month': 'month',
    })
    input_df = input_df[
        (input_df["fiscal_year"] == config.fiscal_year) &
        (input_df["fiscal_month"] == config.fiscal_month)
    ]

    input_df["account_number"] = input_df["gl_account"].astype(str)
    input_tb_df = input_df.merge(
        tb_adj_df[["account_number", "current_movement"]], on=["account_number"], how="left"
    ).reset_index(drop=True)
    #input_tb_df = input_tb_df[input_tb_df.account_number =='51001404002020']
    # Sets 'adjusted_value' to 'current_movement' if available; otherwise uses 'measure_value'
    
    input_tb_df["adjusted_value"] = input_tb_df["current_movement"].astype(float).fillna(input_tb_df["measure_value"].astype(float))

    # Find unique input_ref values where adjustment is not null (i.e., affected by TB adjustment)
   
    affected_inputs = input_tb_df.loc[input_tb_df["current_movement"].notna(), "input_ref"].unique()
    # Filters input_tb_df to only include rows where 'input_ref' is in the list of affected_inputs,
    # i.e., only those input records that have a non-null 'adjusted_value' (impacted by trial balance adjustment).
    input_tb_df = input_tb_df[input_tb_df["input_ref"].isin(affected_inputs)]
    #display (input_tb_df[input_tb_df.account_number == '51001708005800'])
    result = (
        input_tb_df.groupby(["input_ref", "fiscal_month", "fiscal_year"], as_index=False)["adjusted_value"]
        .sum()
        .rename(columns={"adjusted_value": "value"})
    )
    # Group input_tb_df by input_ref, fiscal_month, and fiscal_year,
    # sum the adjusted_value for each group, and rename the column to 'value'.
    
    result["value"] = result["value"].apply(_to_decimal)
    result["input_ref"] = clean_input_ref(result["input_ref"])
    result["fin_period"] = config.fiscal_month
    result["fin_year"] = config.fiscal_year
    result["source"] = "trial balance adjustments"
    display(result)

    return result[INPUT_COLUMNS]


def load_input_adjustments(spark: SparkSession, config: EngineConfig) -> pd.DataFrame:
    """
    Load manual input adjustments from Delta table.
    Returns empty DataFrame if file not found or table is empty.
    """
    print(f"Input_Adj_excel_path:      {config.input_adjustment_excel_path}")
    input_adj_df = write_excel_to_delta(
        spark, config.input_adjustment_excel_path, config.input_adjustment_table, config
    )
    if input_adj_df.empty:
        return pd.DataFrame(columns=INPUT_COLUMNS)
    input_adj_df["input_ref"] = clean_input_ref(input_adj_df["input_ref"])
    input_adj_df["fin_period"] = config.fiscal_month
    input_adj_df["fin_year"] = config.fiscal_year
    input_adj_df["source"] = "manual adjustments"
    return input_adj_df[INPUT_COLUMNS]


def load_kpi_adjustments(spark: SparkSession, config: EngineConfig) -> dict:
    """
    Load KPI-level adjustments from Excel file.
    Returns a dict of key -> adjustment_value.
    """
    columns = ["kpi_id", "kpi_dim_id", "value"]
    print(f"KPI_Adj_excel_path:      {config.kpi_adjustment_excel_path}")
    kpi_adjustment_excel_df = write_excel_to_delta(
        spark, config.kpi_adjustment_excel_path, config.kpi_adjustment_table, config, columns
    )
    if kpi_adjustment_excel_df.empty:
        return {}
    kpi_adjustment_excel_df["kpi_id"] = (
        kpi_adjustment_excel_df["kpi_id"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    kpi_adjustment_excel_df["kpi_dim_id"] = pd.to_numeric(
        kpi_adjustment_excel_df["kpi_dim_id"], errors="coerce"
    )
    kpi_adjustment_excel_df["value"] = pd.to_numeric(
        kpi_adjustment_excel_df["value"], errors="coerce"
    ).fillna(0.0)
    kpi_adjustment_excel_df["key"] = (
        kpi_adjustment_excel_df["kpi_id"] + "_" +
        kpi_adjustment_excel_df["kpi_dim_id"].astype("Int64").astype(str)
    )
    return kpi_adjustment_excel_df.groupby("key")["value"].sum().to_dict()


def load_scr_value_adjustment(spark: SparkSession, config: EngineConfig) -> dict:
    """
    Load KPI-level adjustments from AAA Delta table.
    Returns a dict of key -> adjustment_value.
    """
    kpi_adj_df = spark.table(config.scr_value_adjustment).toPandas()
    month_date = pd.Timestamp(year=config.year, month=config.month, day=1).date()
    kpi_adj_df["month_date"] = pd.to_datetime(kpi_adj_df["month_date"]).dt.date
    kpi_adj_df = kpi_adj_df[(kpi_adj_df["month_date"] == month_date) & (kpi_adj_df["site_code"] == config.site_code)]
    kpi_adj_df = kpi_adj_df[["kpi_reference", "location_column_id", "original_value", "adjusted_value"]]
    kpi_adj_df = kpi_adj_df[kpi_adj_df["adjusted_value"].notnull()]
    kpi_adj_df["value"] = kpi_adj_df["adjusted_value"]
    kpi_adj_df = kpi_adj_df.rename(columns={
        "kpi_reference": "kpi_id",
        "location_column_id": "kpi_dim_id",
    })
    kpi_adj_df["kpi_id"] = kpi_adj_df["kpi_id"].astype(str).str.strip().str.upper()
    kpi_adj_df["kpi_dim_id"] = pd.to_numeric(kpi_adj_df["kpi_dim_id"], errors="coerce")
    kpi_adj_df["value"] = pd.to_numeric(kpi_adj_df["value"], errors="coerce").fillna(0.0)
    kpi_adj_df["key"] = (
        kpi_adj_df["kpi_id"] + "_" +
        kpi_adj_df["kpi_dim_id"].astype("Int64").astype(str)
    )
    print("SCR Value Adjustment loaded from AAA")
    return kpi_adj_df.groupby("key")["value"].sum().to_dict()


def load_all_inputs(spark: SparkSession, config: EngineConfig) -> tuple:
    """
    Orchestrate loading all input data.

    Returns:
        (inputs_df, kpi_master_df, kpi_ref_df, kpi_adjustments)
    """
    print("Loading KPI master from table...")
    kpi_master_df, kpi_ref_df = load_kpi_master(spark, config)

    print("Loading unified inputs...")
    inputs_df = load_unified_inputs(spark, config)

    # Load and merge input-level adjustments
    input_loaders = [
        ("Reconcilor + Manual + Static + Unmatched", load_reconcilor_and_manual),
        ("Historical", load_historical_data),
        ("Input adjustments", load_input_adjustments),
        ("Trial balance adjustments", load_tb_adjustments_aaa),
    ]
    for label, loader in input_loaders:
        print(f"Loading {label}...")
        df = loader(spark, config)
        if not df.empty:
            if label == "Reconcilor + Manual + Static + Unmatched":
                df = df.drop_duplicates(subset=["input_ref"], keep="last")
                inputs_df = inputs_df.drop_duplicates(subset=["input_ref"], keep="last")
            inputs_df = merge_excel_overlay(inputs_df, df)

    # KPI adjustments: use Excel in ba env or for specific period, otherwise load from AAA
    print("Loading KPI adjustments...")
    if config.env == "ba" or (config.fiscal_month == 9 and config.fiscal_year == 2026):
        kpi_adjustments = load_kpi_adjustments(spark, config)
    else:
        kpi_adjustments = load_scr_value_adjustment(spark, config)

    print(f"Total inputs loaded: {len(inputs_df)}")
    return inputs_df, kpi_master_df, kpi_ref_df, kpi_adjustments
