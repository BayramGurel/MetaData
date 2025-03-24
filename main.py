# main.py
import logging
from typing import List, Dict, Any, Optional
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
            ckan.action.package_list()  # Use a valid API call for testing
        else:
            ckan = RemoteCKAN(config.CKAN_API_URL)
            log.info("CKAN connection established without API key (limited access).")
            ckan.action.package_list()  # Use a valid API call
        return ckan
    except errors.CKANAPIError as e:
        log.error("Failed to connect to CKAN: %s", e)
        return None
    except Exception as e:
        log.exception("An unexpected error occurred during CKAN connection: %s", e)
        return None


def run_pipeline() -> None:
    """
    Runs the complete data pipeline.
    """
    ckan = _ckan_connect()
    if not ckan:
        return

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
        # utils.send_notification("Pipeline Failure", f"Critical error: {e}")


if __name__ == "__main__":
    run_pipeline()