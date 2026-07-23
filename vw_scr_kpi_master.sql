"""KPI Calculation Engine - Source Package.

Modules:
    config      - Environment-based configuration (env/*.env)
    utils       - Utility functions for data cleaning
    data_loader - Data loading from Delta tables
    engine      - Core calculation engine logic
"""

from .config import EngineConfig, create_config
from .data_loader import load_all_inputs
from .engine import run_calculation_engine
