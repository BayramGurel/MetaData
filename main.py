import logging
from typing import List, Dict, Any
import config
import utils
import extract_transform
import load_publish
from ckanapi import RemoteCKAN
import argparse  # Although not used, it's good practice to keep it if you plan to add command-line arguments later


log = utils.setup_logging()


def run_pipeline() -> None:
    """Voert de volledige data pipeline uit."""
    try:
        ckan = RemoteCKAN(config.CKAN_API_URL, apikey=config.CKAN_API_KEY)
        print(ckan.action.site_read())  # Add this line for testing

        log.info(f"Start data pipeline for R-drive: {config.R_SCHIJF_PAD}...")
        metadata_lijst: List[Dict[str, Any]] = extract_transform.scan_en_extraheer(config.R_SCHIJF_PAD)

        load_publish.process_extracted_files(ckan, metadata_lijst, config.AUTHORIZATION_MAPPING)

        log.info("Data pipeline voltooid.")

    except Exception as e:
        log.exception("Fout tijdens uitvoeren pipeline: %s", e)


if __name__ == "__main__":
    run_pipeline()