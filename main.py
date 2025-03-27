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
        # ONLY attempt an anonymous connection.
        log.debug(f"CKAN_API_URL from config: {config.CKAN_API_URL}")
        # We intentionally do NOT log the API key now, since we aren't using it.

        ckan = RemoteCKAN(config.CKAN_API_URL)
        log.info("Attempting anonymous connection to CKAN...")
        try:
            result = ckan.action.package_list()
            log.info("Anonymous CKAN connection successful. Result: %s", result)
            return ckan
        except errors.CKANAPIError as e:
            log.error(f"CKAN API error (anonymous connection): {e}")
            if isinstance(e, errors.NotAuthorized):
                log.error("CKAN authorization failed (401). Anonymous access is not allowed.")
            elif isinstance(e, errors.NotFound):
                log.error("CKAN URL not found (404). Double-check CKAN_API_URL.")
            return None  # Explicitly return None on failure
        except Exception as e:
            log.exception("An unexpected error occurred during anonymous CKAN connection: %s", e)
            return None


    except Exception as e:
        log.exception("An unexpected error occurred during CKAN connection setup: %s", e)
        return None

def run_pipeline() -> None:
    """
    Runs the complete data pipeline.
    """
    ckan = _ckan_connect()
    if not ckan:
        log.error("CKAN connection failed.  Exiting pipeline.")
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