# config.py
import os
import logging
from typing import Dict, Optional, Any, ClassVar # Keep ClassVar for MAPPINGS_DIR
from pathlib import Path
import json
import sys # For API key warning output

# --- DotEnv Loading ---
# Needs python-dotenv: pip install python-dotenv
try:
    from dotenv import load_dotenv
    if load_dotenv(): # Returns True if a .env file was found and loaded
        logging.info(".env file loaded.")
    else:
        logging.debug(".env file not found or empty.")
except ImportError:
    logging.warning("python-dotenv not found. .env file support disabled. Install: pip install python-dotenv")

log = logging.getLogger(__name__)

# --- Constants ---
MAPPINGS_DIR = Path(__file__).parent / "mappings" # Assume mappings/ subdirectory

# --- Helper Function to Load Mappings (Same as before) ---
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
            log.info(f"Successfully loaded mapping from {file_path}")
            return mapping
        else:
             log.error(f"Invalid format or value types in mapping file {file_path}.")
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
    Handles loading and providing access to application configuration settings.
    Reads from environment variables (optionally loaded from .env) and JSON mapping files.
    Performs manual validation.
    """

    # --- Core Settings Attributes ---
    R_SCHIJF_PAD: Optional[Path] = None
    TEMP_EXTRACTION_DIR: Path = Path("./temp_extraction").resolve() # Default
    CKAN_API_URL: Optional[str] = None
    CKAN_API_KEY: Optional[str] = None
    LOG_FILE: Path = Path("data_pipeline.log").resolve() # Default
    LOG_LEVEL_STR: str = "INFO" # Default

    # --- Mapping Attributes ---
    CKAN_MAPPING: Dict[str, str] = {}
    CKAN_DATASET_FIELD_MAP: Dict[str, str] = {}
    CKAN_RESOURCE_FIELD_MAP: Dict[str, str] = {}

    def __init__(self):
        """
        Loads configuration manually from environment variables and files.
        """
        log.info("Loading application configuration manually...")

        # --- Load Core Settings ---
        self.R_SCHIJF_PAD = self._load_path("R_SCHIJF_PAD", is_dir=True, is_critical=True)
        # Allow overriding temp dir via env var, use default otherwise
        _temp_dir_str = self._load_str("TEMP_EXTRACTION_DIR", default=None, is_critical=False)
        if _temp_dir_str:
            self.TEMP_EXTRACTION_DIR = Path(_temp_dir_str).resolve()
            log.info(f"Using TEMP_EXTRACTION_DIR from environment: {self.TEMP_EXTRACTION_DIR}")
        else:
            log.info(f"Using default TEMP_EXTRACTION_DIR: {self.TEMP_EXTRACTION_DIR}")
        # Optional: Ensure parent exists here if desired
        # try: self.TEMP_EXTRACTION_DIR.parent.mkdir(parents=True, exist_ok=True)
        # except Exception as e: log.warning(f"Could not ensure TEMP_EXTRACTION_DIR parent exists: {e}")

        self.CKAN_API_URL = self._load_str("CKAN_API_URL", is_critical=True) # Basic validation: must exist
        # Optional: Add URL validation here if needed (e.g., using urllib.parse)
        self.CKAN_API_KEY = self._load_str("CKAN_API_KEY", is_critical=False)

        # Load logging settings
        _log_file_str = self._load_str("LOG_FILE", default=None, is_critical=False)
        if _log_file_str: self.LOG_FILE = Path(_log_file_str).resolve()
        self.LOG_LEVEL_STR = self._load_str("LOG_LEVEL", default="INFO", is_critical=False) or "INFO"

        # --- Load Mappings ---
        # Errors during mapping load are critical and will raise exceptions
        try:
            self.CKAN_MAPPING = _load_mapping_from_json(MAPPINGS_DIR / "general_mapping.json")
            self.CKAN_DATASET_FIELD_MAP = _load_mapping_from_json(MAPPINGS_DIR / "dataset_map.json")
            self.CKAN_RESOURCE_FIELD_MAP = _load_mapping_from_json(MAPPINGS_DIR / "resource_map.json")
            log.info("Successfully loaded field mappings.")
        except (FileNotFoundError, TypeError, ValueError, Exception) as map_load_e:
             log.critical(f"Failed to load required field mappings: {map_load_e}")
             # Re-raise as a RuntimeError to be caught by the singleton block
             raise RuntimeError(f"Failed to load required field mappings: {map_load_e}") from map_load_e

        # --- Post-Loading Checks / Warnings ---
        if self.CKAN_API_URL and not self.CKAN_API_KEY:
            # Use print for critical startup warnings, as logging might not be fully configured
             print(f"WARNING [config]: CKAN_API_KEY is not set for {self.CKAN_API_URL}. Auth operations will fail.", file=sys.stderr)

        log.info("Manual configuration loading complete.")


    # --- Helper Methods for Loading/Validation ---
    def _load_str(self, env_var: str, default: Optional[str] = None, is_critical: bool = False) -> Optional[str]:
        """Loads a string value from environment variables."""
        value = os.getenv(env_var) # Get value from environment
        if value is None:
             value = default # Use default if not found in env

        if is_critical and not value: # Check if critical and still missing
            error_msg = f"CRITICAL: Required environment variable '{env_var}' is not set and no default provided."
            log.critical(error_msg)
            raise ValueError(error_msg)

        log_val = '********' if 'KEY' in env_var.upper() and value else value # Basic masking for keys
        log.debug(f"Config loaded - {env_var}: {log_val if value else 'Not set (using default or optional)'}")
        return value if value else None # Return None if empty string or None

    def _load_path(self, env_var: str, default: Optional[str] = None, is_dir: bool = False, is_critical: bool = False) -> Optional[Path]:
        """Loads a path value, resolves it, and optionally validates."""
        path_str = self._load_str(env_var, default, is_critical)
        if not path_str: return None # Handled by _load_str

        try:
             path_obj = Path(path_str).resolve() # Resolve to absolute path
        except Exception as e:
             error_msg = f"CRITICAL: Invalid path format for '{env_var}' ('{path_str}'): {e}"
             log.critical(error_msg)
             if is_critical: raise ValueError(error_msg) from e
             else: return None # Return None if not critical but path is invalid

        # Perform existence/type validation if required
        if is_critical:
             if not path_obj.exists():
                  error_msg = f"CRITICAL: Required path '{env_var}' does not exist: {path_obj}"
                  log.critical(error_msg)
                  raise ValueError(error_msg)
             if is_dir and not path_obj.is_dir():
                  error_msg = f"CRITICAL: Required path '{env_var}' is not a directory: {path_obj}"
                  log.critical(error_msg)
                  raise ValueError(error_msg)
             elif not is_dir and not path_obj.is_file(): # If check needed for file
                  # Currently no critical file checks needed, but structure is here
                  pass

        log.debug(f"Config loaded - {env_var} Path: {path_obj}")
        return path_obj

    # --- Computed Property ---
    @property
    def LOG_LEVEL(self) -> int:
        """Returns the logging level integer."""
        level_map: Dict[str, int] = {
            "DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING,
            "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL,
        }
        # Use .get with default logging.INFO if string is invalid
        return level_map.get(self.LOG_LEVEL_STR.upper(), logging.INFO)


# --- Singleton Instance ---
# Create a single instance upon import. Errors during instantiation will halt import.
try:
    config = AppConfig()
    # Log summary after successful loading
    log.info("--- Configuration Summary (Manual Load) ---")
    log.info(f"  R_SCHIJF_PAD: {config.R_SCHIJF_PAD}")
    log.info(f"  TEMP_EXTRACTION_DIR: {config.TEMP_EXTRACTION_DIR}")
    log.info(f"  CKAN_API_URL: {config.CKAN_API_URL}")
    log.info(f"  CKAN_API_KEY loaded: {'Yes' if config.CKAN_API_KEY else 'No'}")
    log.info(f"  LOG_FILE: {config.LOG_FILE}")
    log.info(f"  LOG_LEVEL: {logging.getLevelName(config.LOG_LEVEL)}")
    log.info(f"  Mapping files loaded: Yes")
    log.info("-------------------------------------------")

except (ValueError, RuntimeError) as config_load_err: # Catch critical config/mapping errors
    log.critical(f"CRITICAL: Configuration loading failed: {config_load_err}")
    # Re-raise crucial errors to prevent application from running with invalid config
    raise
except Exception as e:
     log.critical(f"CRITICAL: Unexpected error during configuration loading: {e}", exc_info=True)
     raise ValueError("Unexpected error during configuration loading.") from e


# --- Direct Execution Test ---
if __name__ == "__main__":
    print("\n--- Direct execution: Testing manual config loading ---")
    try:
        # Assumes config instance was created successfully above
        print(f"R_SCHIJF_PAD: {config.R_SCHIJF_PAD}")
        print(f"CKAN_API_URL: {config.CKAN_API_URL}")
        print(f"CKAN_API_KEY is set: {bool(config.CKAN_API_KEY)}")
        print(f"Log Level Name: {logging.getLevelName(config.LOG_LEVEL)}")
        print(f"CKAN General Mapping keys loaded: {len(config.CKAN_MAPPING)}")
        print("\nConfig loaded and accessible.")
    except NameError:
        print("\nERROR: Global 'config' instance failed creation.")
    except Exception as e:
        print(f"\nERROR accessing config instance: {e}")