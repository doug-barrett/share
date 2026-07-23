"""
Utility functions for the KPI Calculation Engine.

Contains reusable helper functions for data cleaning, transformation,
notebook initialisation, timing, and Azure SQL type/value utilities.
"""


import time
import math
import pandas as pd
from pathlib import Path
from jinja2 import Template
import numpy as np
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# Unicode subscript digit translator
SUB_MAP = str.maketrans("₀₁₂₃₄₅₆₇₈₉", "0123456789")


def clean_kpi_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean KPI DataFrame by standardizing string columns."""
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
    return df


def clean_input_ref(series: pd.Series) -> pd.Series:
    """
    Standardize input_ref values:
    - Convert to string
    - Strip whitespace
    - Translate unicode subscript digits to normal digits
    """
    return (
        series
        .astype(str)
        .str.strip()
        .str.translate(SUB_MAP)
    )


def merge_excel_overlay(
    base_df: pd.DataFrame,
    overlay_df: pd.DataFrame,
    key_col: str = "input_ref"
) -> pd.DataFrame:
    """
    Merge overlay data into base dataframe:
    - Overwrite existing rows where keys match
    - Append new rows where keys don't exist in base
    """
    base_df = base_df.copy()
    overlay_df = overlay_df.copy()

    # Clean keys on both sides
    base_df[key_col] = clean_input_ref(base_df[key_col])
    overlay_df[key_col] = clean_input_ref(overlay_df[key_col])

    # Deduplicate after normalisation: clean_input_ref() may collapse values that
    # were distinct in the raw data (e.g. whitespace variants, subscript digits),
    # creating duplicate index labels that cause base_df.update() to fail.
    base_df = base_df.drop_duplicates(subset=[key_col], keep="last")
    overlay_df = overlay_df.drop_duplicates(subset=[key_col], keep="last")

    base_df = base_df.set_index(key_col)
    overlay_df = overlay_df.set_index(key_col)

    # Overwrite existing rows
    base_df.update(overlay_df)

    # Append new rows
    new_rows = overlay_df.loc[~overlay_df.index.isin(base_df.index)]
    base_df = pd.concat([base_df, new_rows])

    return base_df.reset_index()

# Convert value to Decimal(36,12) for precise storage in the final table
def _to_decimal(x):
    try:
        f = float(x)
        if not np.isfinite(f):
            return Decimal("0.000000000000")
        return Decimal(str(f)).quantize(Decimal("0.000000000000"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        return Decimal("0.000000000000")

def check_duplicates(df: pd.DataFrame, key_col: str = "input_ref") -> None:
    """Log a warning if duplicate keys are found."""
    dupes = df[df.duplicated(subset=[key_col], keep=False)]
    if not dupes.empty:
        print(f"WARNING: Duplicate {key_col} values found!")
        print(f"Count of duplicate rows: {len(dupes)}")
        print(f"\nTop duplicates:\n{dupes.sort_values(key_col).head(10)}")
    else:
        print(f"No duplicate {key_col} values found")


def create_views(spark, cfg, sql_dir=None) -> list:
    """Render Jinja2-templated SQL view files and execute them via spark.sql.

    Args:
        spark:   Active SparkSession.
        cfg:     EngineConfig instance (provides catalog/schema values).
        sql_dir: Path to the directory containing *.sql files.
                 Defaults to src/sql/ relative to this file.

    Returns:
        List of dicts with keys: view, status, error.
    """

    sql_dir = Path(sql_dir) if sql_dir else Path(__file__).parent / "sql"

    context = {
        "catalog":          cfg.catalog,
        "aaa_catalog":      cfg.aaa_catalog,
        "silver_schema":    cfg.silver_schema,
        "gold_schema":      cfg.gold_schema,
        "reference_schema": cfg.reference_schema,
        "aaa_schema":       cfg.aaa_schema,
        "year":             cfg.year,
        "fiscal_year":      cfg.fiscal_year,
        "month":            cfg.month,
        "fiscal_month":     cfg.fiscal_month,
    }

    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(f"No .sql files found in {sql_dir}")

    print(f"Found {len(sql_files)} SQL file(s):\n")
    results = []
    for sql_file in sql_files:
        view_name = sql_file.stem
        rendered  = Template(sql_file.read_text(encoding="utf-8")).render(**context)
        try:
            spark.sql(rendered)
            results.append({"view": view_name, "status": "SUCCESS", "error": ""})
            print(f"  \u2705  {view_name}")
        except Exception as exc:
            results.append({"view": view_name, "status": "FAILED", "error": str(exc)})
            print(f"  \u274c  {view_name}\n      {exc}")

    ok = sum(r["status"] == "SUCCESS" for r in results)
    print(f"\nDone \u2014 {ok}/{len(results)} view(s) created successfully.")
    return results


class azure_init:

    def __init__(self, config, spark):
        self.config = config
        self.spark = spark
        self.reporting_month: str = config.reporting_month
        self.scenario: str = config.scenario
        self.azsql_schema: str = config.aaa_schema
        self._timings: dict = {}

    # ── Config summary ──────────────────────────────────────────────────────

    def print_config(self) -> None:
        """Print a one-line configuration summary."""
        print(
            f"✓ Config: {self.config.catalog} | "
            f"Azure SQL [{self.azsql_schema}] | "
            f"{self.reporting_month} | {self.scenario}"
        )

    # ── Table verification ───────────────────────────────────────────────────

    def verify_tables(self, tables: list | None = None) -> None:
        """Assert that every (label, fully-qualified-path) pair exists in the catalogue.

        Parameters
        ----------
        tables : list of (str, str), optional
            Sequence of ``(label, table_path)`` pairs to verify.
            Defaults to the ``output``, ``actuals_historic``, and
            ``budget_forecast`` tables from the attached config.

        Raises
        ------
        AssertionError
            If any table path does not exist in the Spark catalogue.
        """
        if tables is None:
            tables = [
                ("output",           self.config.output_table),
                ("actuals_historic", self.config.actuals_historic_table),
                ("budget_forecast",  self.config.budget_forecast_table),
            ]
        for label, path in tables:
            assert self.spark.catalog.tableExists(path), f"Table not found: {path}"
        labels = ", ".join(lbl for lbl, _ in tables)
        print(f"✓ Source tables verified: {labels}")

    # ── Timing helpers ───────────────────────────────────────────────────────

    def start_timer(self, name: str) -> None:
        """Record the wall-clock start time for a named step."""
        self._timings[name] = time.time()

    def end_timer(self, name: str) -> float:
        """Print elapsed minutes for a named step and return the value.

        Returns 0.0 if ``start_timer`` was never called for *name*.
        """
        if name not in self._timings:
            return 0.0
        elapsed = (time.time() - self._timings[name]) / 60
        print(f"  ⏱ {name}: {elapsed:.1f} min")
        return elapsed

    # ── Azure SQL helpers (static — no spark/config dependency) ─────────────

    @staticmethod
    def sanitise(v):
        """Coerce *v* to a form safe for pytds SQL insertion.

        - ``None``, ``NaN``, and ``NaT``  → ``None``
        - NumPy scalars                   → native Python scalar via ``.item()``
        - ``pd.Timestamp``                → ``datetime.datetime``
        - All other values returned unchanged.
        """
        if v is None or isinstance(v, type(pd.NaT)):
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        if hasattr(v, "item"):
            return v.item()
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        return v

    @staticmethod
    def sql_type(dtype) -> str:
        """Map a pandas dtype to its Azure SQL DDL type string.

        ============== ================
        pandas dtype   SQL type
        ============== ================
        float*         DECIMAL(36,12)
        int* / Int*    BIGINT
        datetime*      DATETIME2
        bool           BIT
        other          NVARCHAR(MAX)
        ============== ================
        """
        s = str(dtype)
        if s.startswith("float"):
            return "DECIMAL(36,12)"
        if s.startswith("int") or s.startswith("Int"):
            return "BIGINT"
        if s.startswith("datetime"):
            return "DATETIME2"
        if s == "bool":
            return "BIT"
        return "NVARCHAR(MAX)"

