# utils.py
import logging
import logging.handlers
from typing import Dict, Any
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


def extract_value(data: Dict[str, Any], key: str, nested_key: str = None) -> Any:
    """Haalt een waarde op uit een dictionary, eventueel genest."""
    if nested_key:
        return data.get(nested_key, {}).get(key)
    return data.get(key)

def calculate_file_hash(filepath: str, hash_algorithm: str = "sha256") -> str:
    """Calculates the hash of a file."""
    hasher = hashlib.new(hash_algorithm)
    try:
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(4096)  # Read in chunks
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
    except FileNotFoundError:
        return "" # Or raise the exception, depends if we should continue the process.
    except Exception:
        raise