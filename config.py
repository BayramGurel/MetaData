# config.py
import os
import logging
from typing import Dict, Optional, Any, ClassVar
from pathlib import Path
import json
import sys

# --- Basic Logging Setup ---
# Configure basic logging IMMEDIATELY to catch errors during config loading itself.
# This handler will be removed later by utils.setup_logging if it runs successfully.
logging.basicConfig(level=logging.INFO, format='%(levelname)-8s [%(name)s] %(message)s')

# --- DotEnv Loading ---
try:
    from dotenv import load_dotenv
    if load_dotenv():
        logging.info("(.env file found and loaded)")
    else:
        logging.debug("(.env file not found or empty)")
except ImportError:
    logging.warning("(python-dotenv not found, .env support disabled. Run: pip install python-dotenv)")

log = logging.getLogger(__name__) # Logger for messages specific to this module's logic

# --- Constants ---
# Define paths relative to this config file's location
CONFIG_FILE_PATH = Path(__file__).resolve()
PROJECT_ROOT = CONFIG_FILE_PATH.parent # Assumes config.py is in the project root
# If config.py is in a subdirectory (e.g., 'conf'), use .parent.parent
# PROJECT_ROOT = CONFIG_FILE_PATH.parent.parent
MAPPINGS_DIR = PROJECT_ROOT / "mappings"

# Default paths relative to project root
DEFAULT_TEMP_DIR_PATH = PROJECT_ROOT / "temp_extraction"
DEFAULT_LOG_FILE_PATH = PROJECT_ROOT / "data_pipeline.log"


# --- Helper Function to Load Mappings ---
def _load_mapping_from_json(file_path: Path) -> Dict[str, str]:
    """Loads a mapping dictionary from a JSON file. Raises error if file is invalid/missing."""
    log.debug(f"Attempting to load mapping from: {file_path}")
    if not file_path.is_file():
        log.error(f"Mapping file not found: {file_path}")
        raise FileNotFoundError(f"Required mapping file missing: {file_path}")
    try:
        with file_path.open('r', encoding='utf-8') as f:
            mapping = json.load(f)
        if isinstance(mapping, dict) and all(isinstance(v, str) for v in mapping.values()):
            log.info(f"Successfully loaded mapping from {file_path.name}")
            return mapping
        else:
             log.error(f"Invalid format or value types in mapping file {file_path}. Expected JSON object with string values.")
             raise TypeError(f"Invalid format or values in mapping file {file_path}")
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON mapping file {file_path}: {e}")
        raise ValueError(f"Invalid JSON in mapping file {file_path}") from e
    except Exception as e:
        log.exception(f"Unexpected error loading mapping file {file_path}: {e}")
        raise # Re-raise unexpected errors


# --- Manual Settings Class ---
class AppConfig:
    """
    Handles loading config from environment variables/.env and JSON mapping files.
    Performs manual validation. Access settings via the 'config' instance.
    """

    # --- Declare Attributes with Types and Defaults ---
    R_SCHIJF_PAD: Optional[Path] = None
    TEMP_EXTRACTION_DIR: Path = DEFAULT_TEMP_DIR_PATH
    CKAN_API_URL: Optional[str] = None
    CKAN_API_KEY: Optional[str] = None
    LOG_FILE: Path = DEFAULT_LOG_FILE_PATH
    LOG_LEVEL_STR: str = "INFO"
    CKAN_MAPPING: Dict[str, str] = {}
    CKAN_DATASET_FIELD_MAP: Dict[str, str] = {}
    CKAN_RESOURCE_FIELD_MAP: Dict[str, str] = {}

    def __init__(self):
        """Loads and validates configuration."""
        log.info("Loading application configuration...")

        # --- Load Core Settings ---
        self.R_SCHIJF_PAD = self._load_path("R_SCHIJF_PAD", is_dir=True, is_critical=True)

        # Load temp dir, using default path object if not set in env
        _temp_dir_env = self._load_str("TEMP_EXTRACTION_DIR", is_critical=False)
        if _temp_dir_env:
            self.TEMP_EXTRACTION_DIR = Path(_temp_dir_env).resolve()
            log.info(f"Using TEMP_EXTRACTION_DIR from env: {self.TEMP_EXTRACTION_DIR}")
        else:
            log.info(f"Using default TEMP_EXTRACTION_DIR: {self.TEMP_EXTRACTION_DIR}")

        self.CKAN_API_URL = self._load_str("CKAN_API_URL", is_critical=True)
        self.CKAN_API_KEY = self._load_str("CKAN_API_KEY", is_critical=False)

        # Load log file path, using default path object if not set in env
        _log_file_env = self._load_str("LOG_FILE", is_critical=False)
        if _log_file_env: self.LOG_FILE = Path(_log_file_env).resolve()
        self.LOG_LEVEL_STR = self._load_str("LOG_LEVEL", default="INFO", is_critical=False) or "INFO"

        # --- Load Mappings (Critical) ---
        try:
            self.CKAN_MAPPING = _load_mapping_from_json(MAPPINGS_DIR / "general_mapping.json")
            self.CKAN_DATASET_FIELD_MAP = _load_mapping_from_json(MAPPINGS_DIR / "dataset_map.json")
            self.CKAN_RESOURCE_FIELD_MAP = _load_mapping_from_json(MAPPINGS_DIR / "resource_map.json")
            log.info("Successfully loaded field mappings.")
        except Exception as map_load_e:
             # Error already logged by helper, just raise critical exception
             raise RuntimeError("Failed to load required field mappings") from map_load_e

        # --- Post-Loading Checks ---
        if self.CKAN_API_URL and not self.CKAN_API_KEY:
             # Use print for critical startup warnings before full logging is guaranteed
             print(f"WARNING [config]: CKAN_API_KEY not set for {self.CKAN_API_URL}. Auth required operations will fail.", file=sys.stderr)

        log.info("Manual configuration loading complete.")

    # --- Loading Helpers ---
    def _load_str(self, env_var: str, default: Optional[str] = None, is_critical: bool = False) -> Optional[str]:
        """Loads a string value from environment variables."""
        value = os.getenv(env_var, default) # Uses default if env var not set

        if is_critical and not value: # Check if critical and effectively missing
            error_msg = f"CRITICAL: Required env var '{env_var}' is not set and no default provided."
            log.critical(error_msg); raise ValueError(error_msg)

        # Basic masking for logging
        log_val = '********' if 'KEY' in env_var.upper() and value else value
        # Log only if value is set (or has default), avoid logging "None" constantly
        if value: log.debug(f"Config {env_var}: {log_val}")
        elif default: log.debug(f"Config {env_var}: Not set, using default.")
        else: log.debug(f"Config {env_var}: Not set (Optional).")

        return value if value else None # Return None if empty string or None

    def _load_path(self, env_var: str, default: Optional[str] = None, is_dir: bool = False, is_critical: bool = False) -> Optional[Path]:
        """Loads a path value, resolves it, and validates."""
        path_str = self._load_str(env_var, default, is_critical)
        if not path_str: return None

        try: path_obj = Path(path_str).resolve()
        except Exception as e:
             error_msg = f"Invalid path format for '{env_var}' ('{path_str}'): {e}"
             log.critical(error_msg);
             if is_critical: raise ValueError(error_msg) from e;
             else: return None

        # Perform critical validation
        if is_critical:
             if not path_obj.exists():
                  error_msg = f"CRITICAL: Path '{env_var}' does not exist: {path_obj}"
                  log.critical(error_msg); raise ValueError(error_msg)
             if is_dir and not path_obj.is_dir():
                  error_msg = f"CRITICAL: Path '{env_var}' is not a directory: {path_obj}"
                  log.critical(error_msg); raise ValueError(error_msg)
             # Add is_file check here if needed for other paths

        log.debug(f"Config Path {env_var}: {path_obj} (Exists: {path_obj.exists()})")
        return path_obj

    # --- Computed Property ---
    @property
    def LOG_LEVEL(self) -> int:
        """Returns the logging level integer from LOG_LEVEL_STR."""
        level_map: Dict[str, int] = {
            "DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING,
            "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL,
        }
        return level_map.get(self.LOG_LEVEL_STR.upper(), logging.INFO)

# --- Singleton Instance ---
# Load config immediately upon module import. Critical errors will stop the application.
try:
    config = AppConfig()
    log.info("Application configuration loaded successfully.")
    # Removed verbose summary logging here - rely on debug logs during loading

except (ValueError, RuntimeError, FileNotFoundError) as config_load_err:
    # Catch specific critical errors from loading/validation/mappings
    log.critical(f"CRITICAL: Configuration loading failed: {config_load_err}")
    raise RuntimeError("Application configuration failed to load.") from config_load_err
except Exception as e:
    # Catch any other unexpected errors during instantiation
     log.critical(f"CRITICAL: Unexpected error loading configuration: {e}", exc_info=True)
     raise RuntimeError("Unexpected error loading application configuration.") from e


# --- Direct Execution Test ---
if __name__ == "__main__":
    # Configure basic logging again just for this test block if needed
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s [%(name)s] %(message)s')
    print("\n--- Direct execution: Testing config instance ---")
    try:
        # The 'config' instance should already exist if import succeeded
        print(f"R_SCHIJF_PAD: {config.R_SCHIJF_PAD}")
        print(f"TEMP_EXTRACTION_DIR: {config.TEMP_EXTRACTION_DIR}")
        print(f"CKAN_API_URL: {config.CKAN_API_URL}")
        print(f"CKAN_API_KEY is set: {bool(config.CKAN_API_KEY)}")
        print(f"Log Level Name: {logging.getLevelName(config.LOG_LEVEL)}")
        print(f"CKAN Mapping keys: {len(config.CKAN_MAPPING)}")
        print("\nConfig instance accessible.")
    except NameError:
        print("\nERROR: Global 'config' instance failed creation during module import.")
    except Exception as e:
        print(f"\nERROR accessing config instance: {e}")