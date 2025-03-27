# utils.py
import logging
import logging.handlers
from typing import Dict, Any, Optional, Union
import hashlib
from pathlib import Path
import sys
import os # Added for os.access check

# --- Module Logger ---
# Used for messages originating from the utils module itself,
# especially during setup or for internal warnings.
module_log = logging.getLogger(__name__)

# --- Constants ---
DEFAULT_HASH_CHUNK_SIZE = 65536  # 64 KB - Good balance for I/O

# --- Logging Setup ---
def setup_logging(log_file: Union[str, Path] = "data_pipeline.log",
                  level: int = logging.INFO,
                  logger_name: Optional[str] = None, # None targets root logger
                  log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                  max_bytes: int = 10 * 1024 * 1024, # 10 MB
                  backup_count: int = 5,
                  console_level: Optional[int] = None # Optional separate level for console
                  ) -> logging.Logger:
    """
    Configures logging with rotating file and console handlers.

    Reconfigures the specified logger (or root) if called multiple times.

    Args:
        log_file: Path to the log file.
        level: The minimum logging level for the logger and file handler.
        logger_name: Name of the logger to configure (None for root).
        log_format: Format string for log messages.
        max_bytes: Max size of log file before rotation.
        backup_count: Number of backup log files.
        console_level: Optional separate minimum level for the console handler.
                       Defaults to the main `level` if None.

    Returns:
        The configured logger instance.
    """
    logger = logging.getLogger(logger_name)
    log_path_obj = Path(log_file).resolve() # Resolve path once

    # --- Reset Configuration ---
    # Clear existing handlers to apply the new configuration cleanly.
    if logger.hasHandlers():
        module_log.debug(f"Clearing existing handlers for logger '{logger.name}'.")
        for handler in logger.handlers[:]:
            try: handler.close()
            except Exception: pass
            logger.removeHandler(handler)

    # --- Basic Logger Setup ---
    logger.setLevel(level) # Set the logger's effective level
    formatter = logging.Formatter(log_format)
    if logger_name: logger.propagate = False # Prevent double logging via root

    # --- File Handler ---
    file_handler_error: Optional[str] = None
    try:
        log_path_obj.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_path_obj, maxBytes=max_bytes, backupCount=backup_count,
            encoding='utf-8', delay=True
        )
        file_handler.setFormatter(formatter)
        # File handler level defaults to the logger's level
        logger.addHandler(file_handler)
    except PermissionError:
        file_handler_error = f"Permission denied for log file: {log_path_obj}"
    except Exception as e:
        file_handler_error = f"Failed file handler setup for {log_path_obj}: {e}"

    # --- Console Handler ---
    final_console_level = console_level if console_level is not None else level
    try:
        # Check if a similar console handler already exists (unlikely after clearing, but safe)
        if not any(isinstance(h, logging.StreamHandler) and h.stream == sys.stderr for h in logger.handlers):
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(formatter)
            console_handler.setLevel(final_console_level) # Set specific level for console
            logger.addHandler(console_handler)
    except Exception as e:
        # Fallback to print if even console handler setup fails
        print(f"CRITICAL ERROR: Failed to set up console logging: {e}", file=sys.stderr)

    # --- Log Errors & Confirmation ---
    # Use the configured logger (console handler should exist now) to report errors
    if file_handler_error:
        logger.error(f"File logging disabled. {file_handler_error}")
    # Always log confirmation (or errors)
    logger.info(f"Logging configured for '{logger.name}'. Level: {logging.getLevelName(level)}, Console Level: {logging.getLevelName(final_console_level)}, File: {log_path_obj if not file_handler_error else 'DISABLED'}")

    return logger


# --- Dictionary Value Extraction ---
def extract_value(data: Optional[Dict[str, Any]], key: str, nested_key: Optional[str] = None) -> Optional[Any]:
    """
    Safely extracts a value from a dictionary, optionally nested.

    Args:
        data: The dictionary (or None).
        key: The key to retrieve.
        nested_key: Optional parent key for nested lookup.

    Returns:
        The value, or None if not found or invalid structure.
    """
    # This function is already concise and efficient using .get()
    if not isinstance(data, dict): return None
    try:
        if nested_key:
            # Use .get(nested_key, {}) to handle missing nested_key gracefully
            # before attempting the second .get(key)
            return data.get(nested_key, {}).get(key)
        else:
            return data.get(key)
    except Exception as e: # Should be rare with .get, but catch just in case
        module_log.warning(f"Unexpected error during extract_value(key='{key}', nested='{nested_key}'): {e}")
        return None


# --- File Hashing ---
def calculate_file_hash(filepath: Union[str, Path], hash_algorithm: str = "sha256",
                        chunk_size: int = DEFAULT_HASH_CHUNK_SIZE) -> Optional[str]:
    """
    Calculates the hash of a file efficiently using chunks.

    Args:
        filepath: Path to the file.
        hash_algorithm: Hash algorithm name (e.g., "sha256", "md5").
        chunk_size: Read buffer size in bytes.

    Returns:
        Lowercase hexadecimal hash string, or None on error.
    """
    # Use a specific logger if detailed hash logs are needed, otherwise module_log is fine
    log = module_log # Renamed for consistency within function

    try:
        # Resolve path and perform initial checks before opening
        file_path_obj = Path(filepath).resolve()
        if not file_path_obj.is_file():
             # Handles both "not exists" and "is directory" cases
             log.error(f"Cannot hash: Path is not a file or does not exist: {file_path_obj}")
             return None
        # Check read permission before opening (optional, open() will fail anyway)
        # if not os.access(file_path_obj, os.R_OK):
        #      log.error(f"Cannot hash: Read permission denied: {file_path_obj}")
        #      return None
    except Exception as path_e: # Catch errors during Path() or resolve()
         log.error(f"Invalid filepath for hashing: '{filepath}'. Error: {path_e}")
         return None

    try:
        hasher = hashlib.new(hash_algorithm)
    except ValueError: # Invalid algorithm name
        log.error(f"Invalid hash algorithm: '{hash_algorithm}' for {file_path_obj}")
        return None
    except Exception as hash_init_e: # Other errors initializing hasher
        log.error(f"Failed to init hash algorithm '{hash_algorithm}': {hash_init_e}")
        return None

    try:
        # Open file and read in chunks
        with file_path_obj.open("rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        hex_digest = hasher.hexdigest()
        log.debug(f"Hashed {file_path_obj} ({hash_algorithm}): {hex_digest[:10]}...")
        return hex_digest

    # Specific IOErrors first
    except PermissionError:
         log.error(f"Permission denied reading file for hashing: {file_path_obj}")
         return None
    except OSError as e: # Catches various OS-level I/O errors
         log.error(f"OS error reading file for hashing {file_path_obj}: {e}")
         return None
    # General fallback
    except Exception as e:
        log.exception(f"Unexpected error calculating hash for {file_path_obj}: {e}")
        return None


# --- Example Usage --- (Remains the same, tests the functions)
if __name__ == "__main__":
    root_logger = setup_logging(log_file='utils_test.log', level=logging.DEBUG)
    root_logger.info("--- Testing utils v3 ---") # Use logger instance

    # Test extract_value (no changes needed)
    test_dict = {"a": 1, "b": {"c": 2, "d": None}, "e": None, "f": [1, 2]}
    root_logger.info(f"extract_value(test_dict, 'a'): {extract_value(test_dict, 'a')}")
    root_logger.info(f"extract_value(test_dict, 'c', 'b'): {extract_value(test_dict, 'c', 'b')}")
    root_logger.info(f"extract_value(test_dict, 'd', 'b'): {extract_value(test_dict, 'd', 'b')}")
    root_logger.info(f"extract_value(test_dict, 'f'): {extract_value(test_dict, 'f')}")
    root_logger.info(f"extract_value(test_dict, 'x'): {extract_value(test_dict, 'x')}")
    root_logger.info(f"extract_value(test_dict, 'c', 'e'): {extract_value(test_dict, 'c', 'e')}")
    root_logger.info(f"extract_value(None, 'a'): {extract_value(None, 'a')}")

    # Test hash function
    dummy_file = Path("dummy_hash_test_file_v3.tmp")
    try:
        content = b"Test content for hashing v3." * 500
        dummy_file.write_bytes(content)
        root_logger.info(f"--- Testing calculate_file_hash ---")
        sha256_hash = calculate_file_hash(dummy_file) # Use default sha256
        root_logger.info(f"SHA256 Hash: {sha256_hash}")
        root_logger.info(f"Matches expected: {sha256_hash == hashlib.sha256(content).hexdigest()}")

        # Test error cases
        root_logger.info(f"Hash DNE: {calculate_file_hash('non_existent_file.xyz')}")
        root_logger.info(f"Hash Dir: {calculate_file_hash('.')}")
        root_logger.info(f"Hash Invalid Algo: {calculate_file_hash(dummy_file, 'invalid_algo')}")

    except Exception as e:
         root_logger.exception(f"Error during hash testing: {e}")
    finally:
        dummy_file.unlink(missing_ok=True) # Use missing_ok=True
        root_logger.info(f"Cleaned up {dummy_file}")