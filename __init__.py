"""
Core Calculation Engine for KPI processing.

Implements the formula resolution and evaluation logic,
executing KPIs in wave order based on execution_level.
"""

import re
import pandas as pd
import numpy as np
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Dict, List, Tuple
import hashlib
from datetime import date
from dbruntime.databricks_repl_context import get_context
from .utils import _to_decimal

# Regex patterns for formula parsing
KPI_PATTERN = re.compile(r"(K(?:\d+|JPFY\d+|SPTM\d+|BFYT\d+|PREV\d+))_([0-9]+)")
INPUT_PATTERN = re.compile(r"\bI(\d+)\b")


def build_kpi_ref_dict_map(kpi_ref_dict):
    """
    Build a kpi_ref_dict mapping (int -> str) from either a DataFrame or dict.
    Handles the case where kpi_ref_dict is a DataFrame with columns kpi_dim_id and name.
    """
    if isinstance(kpi_ref_dict, pd.DataFrame):
        return dict(zip(kpi_ref_dict["kpi_dim_id"].astype(int), kpi_ref_dict["name"]))
    elif isinstance(kpi_ref_dict, dict):
        return {int(k): v for k, v in kpi_ref_dict.items()}
    else:
        return {}


def build_lookup(df: pd.DataFrame) -> Dict[Tuple[str, int], float]:
    """Build a lookup dictionary from results DataFrame: (kpi_id, kpi_dim_id) -> value."""
    return {
        (str(r["kpi_id"]).strip().upper(), int(r["kpi_dim_id"])): float(r["value"])
        for _, r in df.iterrows()
        if pd.notna(r["kpi_dim_id"])
    }


def replace_kpis(
    formula: str,
    kpi_lookup: dict,
    input_lookup: dict,
    missing_log: list
) -> str:
    """
    Replace KPI references (K123_4) and input references (I123)
    in a formula string with their resolved numeric values.
    """

    def repl_kpi(match):
        kpi = match.group(1).strip().upper()
        dim = int(match.group(2))
        key_tuple = (kpi, dim)
        key_str = f"{kpi}_{dim}"

        if key_tuple in kpi_lookup:
            return str(kpi_lookup[key_tuple])
        if key_str in kpi_lookup:
            return str(kpi_lookup[key_str])

        missing_log.append({
            "kpi_id": kpi,
            "kpi_dim_id": dim,
            "reason": "Missing KPI dependency \u2192 0"
        })
        return "0"

    formula = KPI_PATTERN.sub(repl_kpi, formula)

    def repl_input(match):
        i_key = f"I{match.group(1)}"
        return str(input_lookup.get(i_key, 0.0))

    formula = INPUT_PATTERN.sub(repl_input, formula)
    return formula


def eval_formula(formula: str) -> float:
    """Safely evaluate a resolved formula string."""
    try:
        result = float(eval(formula))
        return result if np.isfinite(result) else 0.0
    except Exception:
        return 0.0


def build_name_lookup(kpi_master_df: pd.DataFrame) -> dict:
    """Build a KPI ID -> KPI Name lookup."""
    return dict(zip(kpi_master_df["kpi_id"], kpi_master_df["kpi_name"]))


def replace_ids_with_names(formula_series: pd.Series, name_lookup: dict) -> pd.Series:
    """Replace KPI IDs in formulas with human-readable KPI names."""

    def repl(match):
        kpi_id = match.group(1)
        return f"`{name_lookup.get(kpi_id, kpi_id)}`"

    return formula_series.fillna("").str.replace(
        r"(K(?:\d+|JPFY\d+|SPTM\d+|BFYT\d+|PREV\d+))_\d+",
        repl,
        regex=True
    )


def run_calculation_engine(
    kpi_master_df: pd.DataFrame,
    inputs_df: pd.DataFrame,
    kpi_ref_dict,
    kpi_adjustments: dict,
    fiscal_year: int,
    fiscal_month: int,
    year: int,
    month: int,
    base_lookup: dict = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Execute the KPI calculation engine.

    Args:
        kpi_master_df: Master KPI definitions with formulas
        inputs_df: Input reference values
        kpi_ref_dict: Operation index -> name mapping (dict or DataFrame)
        kpi_adjustments: KPI-level adjustments (key -> value)
        fiscal_year: Fiscal year for output tagging
        fiscal_month: Fiscal month for output tagging
        year: Calendar year
        month: Calendar month
        base_lookup: Optional pre-seeded lookup dict of (kpi_id, kpi_dim_id) -> value.
                     Used by historical data load to seed actuals before engine runs.

    Returns:
        Tuple of (results_df, missing_log_df)
    """
    from .utils import clean_kpi_df

    missing_log: List[dict] = []

    # Build kpi_ref_dict map (handles DataFrame or dict)
    kpi_ref_dict_map = build_kpi_ref_dict_map(kpi_ref_dict)
    print("KPI Ref Dict Map")
    print(kpi_ref_dict_map)
    # Prepare KPI master
    kpi_master_df["execution_level"] = pd.to_numeric(
        kpi_master_df["execution_level"], errors="coerce"
    )
    kpi_master_df_clean = kpi_master_df.dropna(subset=["execution_level"]).copy()
    kpi_master_df_clean = clean_kpi_df(kpi_master_df_clean)

    if "active" in kpi_master_df_clean.columns:
        kpi_master_df_clean = kpi_master_df_clean[
            kpi_master_df_clean["active"] == 1
        ].copy()

    # Prepare inputs
    inputs_df = inputs_df.copy()
    inputs_df["input_ref"] = (
        inputs_df["input_ref"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    inputs_df["value"] = pd.to_numeric(inputs_df["value"], errors="coerce")
    input_lookup = dict(zip(inputs_df["input_ref"], inputs_df["value"]))

    # Build results DataFrame
    results_df = kpi_master_df_clean[[
        "kpi_id", "kpi_dim_id", "execution_level",
        "category", "subcategory", "kpi_name", "uom","python_formula","source"
    ]].copy()

    # Add matrix_id
    results_df.insert(
        0, "matrix_id",
        results_df["kpi_id"].astype(str) + "_" +
        results_df["kpi_dim_id"].astype("Int64").astype(str)
    )

    # Add sort_key
    results_df.insert(
        results_df.columns.get_loc("kpi_dim_id") + 1,
        "sort_key",
        results_df["kpi_id"].str.extract(r"(\d+)").astype(float)
    )

    # Add dim_name using kpi_ref_dict_map
    results_df.insert(
        results_df.columns.get_loc("kpi_dim_id") + 1,
        "dim_name",
        results_df["kpi_dim_id"].astype(int).map(kpi_ref_dict_map)
    )

    # Initialize output columns
    results_df["value"] = 0.0
    results_df["resolved_formula"] = ""

    # Add resolved formula with names
    name_lookup = build_name_lookup(kpi_master_df)
    results_df["resolved_formula_names"] = replace_ids_with_names(
        results_df["python_formula"], name_lookup
    )

    # Execute in waves by execution_level
    execution_levels = sorted(results_df["execution_level"].unique())
    print(f"Executing {len(execution_levels)} levels: {execution_levels}")

    for level in execution_levels:
        if base_lookup:
            lookup = dict(base_lookup)
            lookup.update(build_lookup(results_df))
        else:
            lookup = build_lookup(results_df)
        lookup.update(input_lookup)

        level_mask = results_df["execution_level"] == level
        level_rows = results_df[level_mask]

        updates = []
        for idx, row in level_rows.iterrows():
            formula = row["python_formula"]

            if isinstance(formula, str) and formula.strip() != "":
                resolved = replace_kpis(formula, lookup, input_lookup, missing_log)
                value = eval_formula(resolved)
            else:
                resolved = ""
                value = 0.0

            # Apply KPI adjustments
            adj_key = f"{row['kpi_id']}_{int(row['kpi_dim_id'])}"
            if adj_key in kpi_adjustments:
                adj_val = kpi_adjustments[adj_key]
                value += adj_val
                if resolved:
                    resolved = f"({resolved}) {adj_val:+.15f} [ADJ]"
                else:
                    resolved = f"{adj_val:+.15f} [ADJ]"

            updates.append((idx, value, resolved))

        for idx, val, resolved in updates:
            results_df.at[idx, "value"] = float(val)
            results_df.at[idx, "resolved_formula"] = resolved

    results_df["kpi_engine_key"] = results_df["matrix_id"].fillna("NA").astype(str) + "||" + str(pd.Timestamp(year=year, month=month, day=1).date())
    results_df["kpi_engine_key"] = results_df["kpi_engine_key"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    # {month:02d} formats the integer 'month' as a two-digit string, padding with zero if needed (e.g., 5 becomes '05')
    results_df["date_key"] = pd.Timestamp(year=year, month=month, day=1).date()
    # Add fiscal period to results
    results_df["fiscal_year"] = fiscal_year
    results_df["fiscal_month"] = fiscal_month
    results_df["created_date"] = pd.Timestamp.now()
    results_df["created_by"] = get_context().user
    
    results_df = results_df[
    [
        "kpi_engine_key", "matrix_id", "date_key", "fiscal_year", "fiscal_month",
        "kpi_id", "kpi_dim_id", "dim_name", "sort_key", "execution_level", "category", "subcategory",
        "kpi_name", "python_formula", "uom", "value", "resolved_formula","source","resolved_formula_names","created_date","created_by"
    ]
    ]


  
    # Build missing log
    missing_log_df = pd.DataFrame(missing_log)
    skipped_count = kpi_master_df[kpi_master_df["execution_level"].isna()].shape[0]
    

    
    results_df["value"] = results_df["value"].apply(_to_decimal)
    print(f"\nCalculation complete:")
    print(f"  - KPIs calculated: {len(results_df)}")
    print(f"  - Missing dependencies: {len(missing_log_df)}")
    print(f"  - Skipped (no execution level): {skipped_count}")

    return results_df, missing_log_df
