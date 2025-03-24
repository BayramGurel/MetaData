import logging
import logging.handlers
from typing import Dict, Any, Optional, Union
import hashlib

def setup_logging(log_file: str = "data_pipeline.log", level: int = logging.INFO) -> logging.Logger:
    """
    Sets up logging to both a rotating file and the console.

    Args:
        log_file: The name of the log file.
        level: The logging level (e.g., logging.DEBUG, logging.INFO).

    Returns:
        The configured logger.
    """
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def extract_value(data: Dict[str, Any], key: str, nested_key: Optional[str] = None) -> Optional[Any]:
    """
    Safely extracts a value from a dictionary, handling nested keys.

    Args:
        data: The dictionary to extract from.
        key: The key to look for.
        nested_key:  An optional nested key (if the value is within a sub-dictionary).

    Returns:
        The value associated with the key, or None if not found.  Can return any type.
    """
    if nested_key:
        nested_data = data.get(nested_key)
        if isinstance(nested_data, dict):  # Check if nested_data is a dictionary
            return nested_data.get(key)
        return None  # Return None if nested_key doesn't point to a dict
    return data.get(key)


def calculate_file_hash(filepath: str, hash_algorithm: str = "sha256") -> Optional[str]:
    """
    Calculates the hash of a file.

    Args:
        filepath: The path to the file.
        hash_algorithm: The hashing algorithm to use (default: SHA256).

    Returns:
        The hexadecimal representation of the hash, or None if an error occurred.
    """
    log = logging.getLogger(__name__)  # Get logger *inside* the function.
    try:
        hasher = hashlib.new(hash_algorithm)
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(4096)  # Read in chunks
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
    except FileNotFoundError:
        log.error(f"File not found: {filepath}")  # Use log.error for consistency
        return None
    except Exception as e:
        log.exception(f"Error calculating hash for {filepath}: {e}")
        return None  # Return None on *any* error, not just FileNotFoundError