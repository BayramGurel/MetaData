# main.py
import logging
from typing import List, Dict, Any
from urllib.parse import urlparse
import shutil # <<< --- ADD THIS IMPORT ---
import sys # <<< --- ADD THIS IMPORT (if not already there) ---
import time # Example if needing delays (not strictly needed now)

# ... (Keep other imports and logging setup) ...
try:
    import config
    import utils
except (ImportError, ValueError, RuntimeError) as config_err:
    print(f"CRITICAL [main]: Failed to load configuration or utils: {config_err}", file=sys.stderr)
    sys.exit(1)
try:
    from extract_transform import MetadataScanner, CkanTransformer, MetadataValidator
    from load_publish import CkanUploader
except ImportError as import_err:
    print(f"CRITICAL [main]: Failed to import necessary classes: {import_err}", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
try:
    log = utils.setup_logging(log_file=config.config.LOG_FILE, level=config.config.LOG_LEVEL, logger_name="DataPipeline")
except Exception as log_err:
     print(f"CRITICAL [main]: Failed to set up logging: {log_err}", file=sys.stderr)
     logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s: %(message)s')
     log = logging.getLogger("DataPipeline_Fallback"); log.error(f"Logging setup failed: {log_err}.")

# --- Main Pipeline Class ---
class DataPipeline:
    """Orchestrates the data extraction, transformation, validation, and loading."""
    def __init__(self):
        log.info("Initializing Data Pipeline components...")
        try:
            self.scanner = MetadataScanner(
                root_scan_path=config.config.R_SCHIJF_PAD,
                temp_dir=config.config.TEMP_EXTRACTION_DIR
            )
            self.validator = MetadataValidator()
            self.transformer = CkanTransformer(ckan_mapping=config.config.CKAN_MAPPING)
            self.uploader = CkanUploader(
                ckan_api_url=str(config.config.CKAN_API_URL),
                ckan_api_key=config.config.CKAN_API_KEY
            )
        except Exception as e:
             log.exception("CRITICAL: Failed to initialize pipeline components.")
             raise RuntimeError("Pipeline component initialization failed.") from e

    def run(self) -> None:
        """Executes the full data pipeline."""
        log.info("="*20 + " Starting Data Pipeline Run " + "="*20)
        scan_temp_dir_to_clean: Optional[Path] = None # <<< Keep track of dir path

        try: # <<< Main execution block
            if not self.uploader.is_connected():
                log.critical("CKAN connection failed. Pipeline cannot continue.")
                return

            # --- 1. Scan and Extract ---
            scan_path = config.config.R_SCHIJF_PAD
            log.info(f"Starting scan of directory: {scan_path}")
            # Get the temp dir used by the scanner for potential cleanup
            scan_temp_dir_to_clean = self.scanner.run_temp_dir # <<< Assign before scan potentially changes it
            extracted_metadata: List[Dict[str, Any]] = self.scanner.scan()
            # --- Store temp dir path AGAIN after scan, in case scan re-initializes it ---
            scan_temp_dir_to_clean = self.scanner.run_temp_dir # <<< Get path used by *this* scan

            if not extracted_metadata:
                log.info("Scan complete: No processable items found.")
                # No need to run rest of pipeline or cleanup if nothing was scanned/extracted
                return # Exit cleanly

            log.info(f"Scan complete: Found {len(extracted_metadata)} initial items.")

            # --- 2. Validate ---
            # ... (validation loop remains the same) ...
            log.info("Starting metadata validation...")
            items_to_process: List[Dict[str, Any]] = []
            validation_issue_count = 0
            for item_meta in extracted_metadata:
                rel_path = item_meta.get('relative_path', item_meta.get('bestandspad', 'Unknown'))
                issues = self.validator.validate(item_meta)
                if issues:
                     validation_issue_count += 1
                     log.warning(f"Validation issues for '{rel_path}': {issues}")
                items_to_process.append(item_meta)
            if validation_issue_count > 0: log.warning(f"Validation complete: Issues in {validation_issue_count} item(s).")
            else: log.info("Validation complete: No issues found.")


            # --- 3. Load and Publish ---
            log.info(f"Starting upload process for {len(items_to_process)} items...")
            try:
                ckan_url_str = config.config.CKAN_API_URL or ""
                ckan_host = urlparse(ckan_url_str).hostname
                if ckan_host: log.info(f"... Target CKAN instance: {ckan_host}")
                else: log.warning(f"Could not determine hostname from CKAN URL: {ckan_url_str}")
            except Exception: log.warning("Could not parse CKAN URL host.")

            # Call the uploader
            resource_count, dataset_count = self.uploader.process_list(
                metadata_list=items_to_process, # Pass potentially filtered list if validation skips items
                transformer=self.transformer,
                dataset_field_map=config.config.CKAN_DATASET_FIELD_MAP,
                resource_field_map=config.config.CKAN_RESOURCE_FIELD_MAP
            )

            log.info(f"Upload process complete. Processed {resource_count} resources across {dataset_count} unique datasets.")
            log.info("="*20 + " Data Pipeline Run Finished Successfully " + "="*20)

        except Exception as e:
            log.exception("CRITICAL: An unexpected error occurred during pipeline execution:")
            log.info("="*20 + " Data Pipeline Run Failed " + "="*20)
            # Re-raise the exception if you want the main entry point to catch it
            # raise

        finally: # <<< --- ADD FINALLY BLOCK FOR CLEANUP ---
             # --- Cleanup Scanner's Temp Directory ---
             if scan_temp_dir_to_clean and scan_temp_dir_to_clean.exists():
                  log.info(f"Cleaning up main scan temporary directory: {scan_temp_dir_to_clean}")
                  try:
                       shutil.rmtree(scan_temp_dir_to_clean, ignore_errors=True)
                       log.info("Main scan temporary directory cleaned.")
                  except Exception as clean_e:
                       # Log error but don't crash the pipeline just for cleanup failure
                       log.error(f"Error cleaning up scan temp dir {scan_temp_dir_to_clean}: {clean_e}")
             elif scan_temp_dir_to_clean:
                  log.debug(f"Scan temporary directory already removed or not created: {scan_temp_dir_to_clean}")
             else:
                  log.debug("No scan temporary directory path found to clean up.")


# --- Script Entry Point --- (Keep existing improved version)
if __name__ == "__main__":
    pipeline_instance = None
    exit_code = 0 # Default success
    try:
        pipeline_instance = DataPipeline()
    except Exception as init_err:
         err_msg = f"CRITICAL [main]: Pipeline initialization failed: {init_err}"
         print(err_msg, file=sys.stderr); log.critical(err_msg, exc_info=True); exit_code = 1
    if pipeline_instance:
        try:
            pipeline_instance.run()
        except Exception as run_err:
            err_msg = f"CRITICAL [main]: Unhandled exception during pipeline execution: {run_err}"
            print(err_msg, file=sys.stderr); log.exception(err_msg); exit_code = 1
    sys.exit(exit_code) # Exit with appropriate code