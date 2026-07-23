# --- Development environment ---
CATALOG=enterprise_data_platform_analysis
LANDING_SCHEMA=landing
BRONZE_SCHEMA=bronze
SILVER_SCHEMA=silver
GOLD_SCHEMA=gold
REFERENCE_SCHEMA=reference_data
MANUAL_SCHEMA=manual_data
VOLUME_BASE_PATH=/Volumes/enterprise_data_platform_analysis/landing/dataconnect/mungari

# --- AAA / Azure SQL ---
AAA_CATALOG=aaa_sqlmi_dev
AAA_SCHEMA=edp_aaa
SCENARIO=ACTUAL
AZSQL_SERVER=sql-scr-triplea-dev.database.windows.net
AZSQL_DATABASE=scr-triplea-dev-db
AZSQL_TENANT_ID=6901ca36-3d54-46a8-a4e5-9bc1c5f5b409
AZSQL_CLIENT_ID=e1762a5c-ab1f-4db5-b72f-cd99482ddb58
AZSQL_SECRET_SCOPE=keyvault-managed
AZSQL_SECRET_KEY=aad-spn-evn-scr-triplea-dbe-power-platform

HISTORICAL_BF_FILE=historical_budget_forecast/scr_historicals_budget_forecast_2026.xlsx
SCR_PRONTO_MAPPING_FILE=pronto/scr_pronto_mapping_sheet.xlsx
PBI_TEMPLATES_FILE=powerbi_templates/pbi_report_templates.xlsx

# --- Excel source paths (relative to VOLUME_BASE_PATH) ---
# Update these each reporting period to point to the latest files
RECONCILOR_FILE=reconcilor_data/scr_reconcilor_data
MANUAL_FILE=business_manual_data/scr_business_manual_data
HISTORICAL_FILE=historical_input_ref/scr_historical_input_data
UNMATCHED_FILE=scr_data/scr_nonmatched_kpi
STATIC_FILE=static_data/scr_static_data
TB_ADJUSTMENT_FILE=adjustments/scr_trial_balance_adjustment_data
KPI_ADJUSTMENT_FILE=adjustments/scr_kpi_adjustment
INPUT_ADJUSTMENT_FILE=adjustments/scr_input_adjustment_data
