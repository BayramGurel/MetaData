# main.py
import logging
from typing import List, Dict, Any, Optional  # Import Optional
import config
import utils
import extract_transform
import load_publish
from ckanapi import RemoteCKAN, errors

log = utils.setup_logging()


def _ckan_connect() -> Optional[RemoteCKAN]:
    """Establishes a connection to CKAN, handling API key and connection tests."""
    try:
        if config.CKAN_API_KEY:
            ckan = RemoteCKAN(config.CKAN_API_URL, apikey=config.CKAN_API_KEY)
            log.info("CKAN connection established with API key.")
            ckan.action.site_read()  # Test API key
        else:
            ckan = RemoteCKAN(config.CKAN_API_URL)
            log.info("CKAN connection established without API key (limited access).")
            ckan.action.site_read()  # Test connection
        return ckan
    except errors.CKANAPIError as e:
        log.error("Failed to connect to CKAN: %s", e)
        return None
    except Exception as e:
        log.exception("An unexpected error occurred during CKAN connection: %s", e)
        return None


def run_pipeline() -> None:
    """
    Runs the complete data pipeline:
    1. Connects to CKAN.
    2. Scans and extracts metadata from the R-drive.
    3. Processes (loads/updates/publishes) the extracted metadata to CKAN.
    4. Handles errors gracefully.
    """
    ckan = _ckan_connect()
    if not ckan:
        return  # Exit if CKAN connection failed

    log.info(f"Starting data pipeline for R-drive: {config.R_SCHIJF_PAD}...")

    try:
        metadata_lijst: List[Dict[str, Any]] = extract_transform.scan_en_extraheer(
            config.R_SCHIJF_PAD
        )
        if not metadata_lijst:
            log.info("No files found to process. Pipeline exiting.")
            return

        load_publish.process_extracted_files(ckan, metadata_lijst)

        log.info("Data pipeline completed successfully.")

    except Exception as e:
        log.exception("Critical error during pipeline execution: %s", e)
        # utils.send_notification("Pipeline Failure", f"Critical error: {e}")  # Example


if __name__ == "__main__":
    run_pipeline()