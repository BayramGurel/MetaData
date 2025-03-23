import logging
import logging.handlers
from typing import Dict, Any, Optional, Union
import hashlib

def setup_logging(log_file: str = "data_pipeline.log", level: int = logging.INFO) -> logging.Logger:
    """Configureert logging naar bestand en console."""
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def extract_value(data: Dict[str, Any], key: str, nested_key: str = None) -> Optional[Union[str, int, float, list, dict]]:
    """Haalt een waarde op uit een dictionary, eventueel genest.

    Example:
        data = {"a": 1, "b": {"c": 2}}
        extract_value(data, "c", "b")  # Returns 2
        extract_value(data, "a")  # Returns 1
        extract_value(data, "d")  # Returns None
        extract_value(data, "d", "b") # Returns None
    """
    if nested_key:
        return data.get(nested_key, {}).get(key)
    return data.get(key)

def calculate_file_hash(filepath: str, hash_algorithm: str = "sha256") -> str:
    """Calculates the hash of a file."""
    log = logging.getLogger(__name__) # Best practice: get logger inside function
    hasher = hashlib.new(hash_algorithm)
    try:
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(4096)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
    except FileNotFoundError:
        log.warning(f"File not found: {filepath}")
        return ""
    except Exception as e:
        log.exception(f"Error calculating hash for {filepath}: {e}")
        raise