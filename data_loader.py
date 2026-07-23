"""Configuration for the KPI Calculation Engine.

Values are loaded from env/<env>.env files. Override via Job parameters at runtime.
"""

import glob as _glob
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

# Env files live in the env/ folder, one level above src/ then into env/.
# Use os.path for compatibility with Databricks serverless.
_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_DIR = Path(os.path.join(_SRC_DIR, "..", "env"))


def _load_env_file(env: str) -> dict:
    """Parse <env>.env from the env/ folder and return a key->value dict.

    Lines starting with '#' and blank lines are ignored.
    Raises FileNotFoundError if the requested env file does not exist.
    """
    env_file = _ENV_DIR / f"{env}.env"
    if not env_file.exists():
        raise FileNotFoundError(
            f"Environment file not found: {env_file}\n"
            f"Supported environments: dev, sit, uat, prod"
        )
    result: dict = {}
    with open(env_file) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                result[key.strip()] = value.strip()
    return result


def _derive_fiscal_period(year: int, month: int) -> tuple:
    """Derive fiscal_year and fiscal_month from calendar year/month.

    Assumes a July-June fiscal year (Evolution Mining standard):
      - FY month 1 = July, FY month 12 = June
      - FY label = the calendar year in which June falls
        e.g. July 2025 - June 2026 -> FY2026, fiscal_month for July = 1
    """
    if month >= 7:
        fiscal_year = year + 1
        fiscal_month = month - 6
    else:
        fiscal_year = year
        fiscal_month = month + 6
    return fiscal_year, fiscal_month


@dataclass
class EngineConfig:
    """Runtime configuration for the KPI Calculation Engine."""

    # --- Environment ---
    env: str

    # --- Catalog & Schema ---
    catalog: str
    aaa_catalog: str
    landing_schema: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    reference_schema: str
    manual_schema: str
    aaa_schema: str
    scenario: str

    # --- Azure SQL Auth ---
    azsql_server: str
    azsql_database: str
    azsql_tenant_id: str
    azsql_client_id: str
    azsql_secret_scope: str
    azsql_secret_key: str

    # --- Volume Path ---
    volume_base_path: str

    # --- site code ---
    site_code: str

    # --- Period (from params or today) ---
    fiscal_year: int
    fiscal_month: int
    month: int
    year: int

    # --- Excel source files ---
    reconcilor_file: str
    manual_file: str
    historical_file: str
    unmatched_file: str
    static_file: str
    input_adjustment_file: str
    tb_adjustment_file: str
    kpi_adjustment_file: str
    historical_bf_file: str
    pronto_mapping_file: str
    pbi_templates_file: str
    # --- Derived: Reporting Month ---
    @property
    def reporting_month(self) -> str:
        """Reporting month as YYYY-MM-01 string."""
        return f"{self.year}-{self.month:02d}-01"

    # --- Table Names (fully qualified) ---
    @property
    def kpi_master_table(self) -> str:
        return f"{self.catalog}.{self.reference_schema}.scr_mgo_kpi_master"

    @property
    def scr_location_hierarchy_table(self) -> str:
        return f"{self.catalog}.{self.reference_schema}.scr_location_hierarchy"

    @property
    def kpi_ref_table(self) -> str:
        return f"{self.catalog}.{self.reference_schema}.scr_mgo_kpi_ref"

    @property
    def unified_input_ref_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.unified_input_ref"

    @property
    def input_to_gl_account_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.stg_inputs_to_gl_monthly_activity"

    @property
    def reconcilor_table(self) -> str:
        return f"{self.catalog}.{self.manual_schema}.scr_reconcilor_data"

    @property
    def manual_table(self) -> str:
        return f"{self.catalog}.{self.manual_schema}.scr_manual_data"

    @property
    def historical_table(self) -> str:
        return f"{self.catalog}.{self.manual_schema}.scr_historical_input_data"

    @property
    def scr_unmatched_table(self) -> str:
        return f"{self.catalog}.{self.manual_schema}.scr_unmatched_data"

    @property
    def static_table(self) -> str:
        return f"{self.catalog}.{self.manual_schema}.scr_static_data"

    @property
    def kpi_adjustment_table(self) -> str:
        return f"{self.catalog}.{self.manual_schema}.scr_kpi_adjustment_data"
    
    @property
    def input_adjustment_table(self) -> str:
        return f"{self.catalog}.{self.manual_schema}.scr_input_adjustment_data"

    @property
    def tb_adjustment_table(self) -> str:
        return f"{self.catalog}.{self.manual_schema}.scr_tb_adjustment_data"

    @property
    def scr_value_adjustment(self) -> str:
        return f"{self.aaa_catalog}.{self.aaa_schema}.scr_value_adjustment"

    @property
    def output_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.scr_calculation_engine_output_current"

    @property
    def scr_history_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.scr_calculation_engine_output"

    @property
    def actuals_historic_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.scr_actuals_historic"

    @property
    def budget_forecast_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.scr_budget_forecast"

    @property
    def enriched_view(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.vw_scr_calculation_engine_output_current_enriched"
    
    @property
    def scr_trial_bal_mvmt_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.scr_trial_bal_mvmt"
    
    @property
    def gl_history_period_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.pronto_gl_history_period"
    
    @property
    def gl_master_period_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.pronto_gl_master"
    
    @property
    def scr_trial_balance_table(self) -> str:
        return f"{self.aaa_catalog}.{self.aaa_schema}.scr_trial_balance"

    # --- Excel path resolver ---
    def _find_excel(self, file_path: str) -> str | None:
        """Find an Excel file in the year/month volume directory.

        Resolves the folder name from the leading path component of
        ``file_path`` (e.g. ``"reconcilor_data"`` from
        ``"reconcilor_data/scr_reconcilor_data_may_2026.xlsx"``) and
        searches for any ``*.xlsx`` file under:

            {volume_base_path}/{year}/{month:02d}/{folder}/

        where ``year`` and ``month`` come from the config parameters,
        so switching the reporting period automatically picks up the
        correct file without touching the env file.

        Returns the first alphabetical match, or None if not found.
        """
        parts = file_path.split("/", 1)
        folder = parts[0]
        file_prefix = parts[1] if len(parts) > 1 else ""
        directory = f"{self.volume_base_path}/{self.year}/{self.month:02d}/{folder}"
        pattern = f"{directory}/{file_prefix}*.xlsx" if file_prefix else f"{directory}/*.xlsx"
        matches = sorted(_glob.glob(pattern))
        if not matches:
            return None
        return matches[0]

    # --- Derived Excel Paths ---
    @property
    def historical_bf_excel_path(self) -> str | None:
        if not self.historical_bf_file:
            return None
        return f"{self.volume_base_path}/{self.historical_bf_file}"
    @property
    def pronto_mapping_excel_path(self) -> str | None:
        return f"{self.volume_base_path}/{self.pronto_mapping_file}"

    @property
    def pbi_templates_excel_path(self) -> str | None:
         return f"{self.volume_base_path}/{self.pbi_templates_file}"

    @property
    def reconcilor_excel_path(self) -> str | None:
        return self._find_excel(self.reconcilor_file)

    @property
    def manual_excel_path(self) -> str | None:
        return self._find_excel(self.manual_file)

    @property
    def historical_excel_path(self) -> str | None:
        return self._find_excel(self.historical_file)

    @property
    def input_adjustment_excel_path(self) -> str | None:
        return self._find_excel(self.input_adjustment_file)

    @property
    def scr_unmatched_excel_path(self) -> str | None:
        return self._find_excel(self.unmatched_file)

    @property
    def scr_static_excel_path(self) -> str | None:
        return self._find_excel(self.static_file)

    @property
    def tb_adjustment_excel_path(self) -> str | None:
        return self._find_excel(self.tb_adjustment_file)

    @property
    def kpi_adjustment_excel_path(self) -> str | None:
        return self._find_excel(self.kpi_adjustment_file)


def create_config(params: dict) -> EngineConfig:
    """Create an EngineConfig from job parameters / widget values.

    Resolution order for infrastructure fields:
      1. Widget / job param (non-empty string wins)
      2. Value from <env>.env file
      3. Hardcoded fallback default

    The 'env' key in params selects which .env file to load
    (dev | sit | uat | prod). Defaults to 'dev'.

    Run-period fields:
      - 'year' and 'month' come from job params; default to today's date.
      - 'fiscal_year' and 'fiscal_month' are derived automatically from
        year/month using the July-June fiscal calendar.
    """
    env = (params.get("env") or "dev").strip()
    env_vals = _load_env_file(env)

    def _get(param_key: str, env_key: str, default: str) -> str:
        widget = (params.get(param_key) or "").strip()
        return widget or env_vals.get(env_key, default)

    # Period: from params or default to today
    today = date.today()
    year  = int(params.get("year") )
    month = int(params.get("month"))
    site_code = params.get("site_code")
    fiscal_year, fiscal_month = _derive_fiscal_period(year, month)

    return EngineConfig(
        env=env,
        site_code=site_code,    
        catalog=_get("catalog", "CATALOG", "enterprise_data_platform_dev"),
        aaa_catalog=_get("aaa_catalog", "AAA_CATALOG", "aaa_sqlmi_dev"),
        landing_schema=_get("landing_schema", "LANDING_SCHEMA", "bronze"),
        bronze_schema=_get("bronze_schema", "BRONZE_SCHEMA", "bronze"),
        silver_schema=_get("silver_schema", "SILVER_SCHEMA", "silver"),
        gold_schema=_get("gold_schema", "GOLD_SCHEMA", "gold"),
        reference_schema=_get("reference_schema", "REFERENCE_SCHEMA", "reference_data"),
        manual_schema=_get("manual_schema", "MANUAL_SCHEMA", "manual_data"),
        aaa_schema=_get("aaa_schema", "AAA_SCHEMA", "edp_aaa"),
        scenario=_get("scenario", "SCENARIO", "ACTUAL"),
        azsql_server=_get("azsql_server", "AZSQL_SERVER", ""),
        azsql_database=_get("azsql_database", "AZSQL_DATABASE", ""),
        azsql_tenant_id=_get("azsql_tenant_id", "AZSQL_TENANT_ID", ""),
        azsql_client_id=_get("azsql_client_id", "AZSQL_CLIENT_ID", ""),
        azsql_secret_scope=_get("azsql_secret_scope", "AZSQL_SECRET_SCOPE", "keyvault-managed"),
        azsql_secret_key=_get("azsql_secret_key", "AZSQL_SECRET_KEY", ""),
        volume_base_path=_get("volume_base_path", "VOLUME_BASE_PATH", ""),
        fiscal_year=fiscal_year,
        fiscal_month=fiscal_month,
        month=month,
        year=year,
        reconcilor_file=_get("reconcilor_file", "RECONCILOR_FILE", ""),
        manual_file=_get("manual_file", "MANUAL_FILE", ""),
        historical_file=_get("historical_file", "HISTORICAL_FILE", ""),
        unmatched_file=_get("unmatched_file", "UNMATCHED_FILE", ""),
        static_file=_get("static_file", "STATIC_FILE", ""),
        tb_adjustment_file=_get("tb_adjustment_file", "TB_ADJUSTMENT_FILE", ""),
        input_adjustment_file=_get("input_adjustment_file", "INPUT_ADJUSTMENT_FILE", ""),
        kpi_adjustment_file=_get("kpi_adjustment_file", "KPI_ADJUSTMENT_FILE", ""),
        historical_bf_file=_get("historical_bf_file", "HISTORICAL_BF_FILE", ""),
        pronto_mapping_file=_get("pronto_mapping_file", "SCR_PRONTO_MAPPING_FILE", ""),
        pbi_templates_file=_get("pbi_templates_file", "PBI_TEMPLATES_FILE", ""),
    )
