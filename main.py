# main.py
import logging
from typing import List, Dict, Any # Keep basic types if needed
from urllib.parse import urlparse # <<< --- ADD THIS IMPORT ---

# --- Module Imports ---
# Import the singleton config object and utility function
try:
    import config # Import the module to access the singleton 'config' instance
    import utils
except (ImportError, ValueError, RuntimeError) as config_err:
    import sys
    print(f"CRITICAL [main]: Failed to load configuration or utils: {config_err}", file=sys.stderr)
    sys.exit(1)

# Import the necessary classes from refactored modules
try:
    from extract_transform import MetadataScanner, CkanTransformer, MetadataValidator
    from load_publish import CkanUploader
except ImportError as import_err:
    import sys
    print(f"CRITICAL [main]: Failed to import necessary classes: {import_err}", file=sys.stderr)
    print("Ensure extract_transform.py and load_publish.py are correctly refactored and accessible.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
# Setup logging using the configuration loaded by the config module
try:
    log = utils.setup_logging(
        log_file=config.config.LOG_FILE,
        level=config.config.LOG_LEVEL,
        logger_name="DataPipeline"
    )
except Exception as log_err:
     import sys
     print(f"CRITICAL [main]: Failed to set up logging: {log_err}", file=sys.stderr)
     logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s: %(message)s')
     log = logging.getLogger("DataPipeline_Fallback")
     log.error(f"Logging setup failed: {log_err}. Using basic logging.")


# --- Main Pipeline Class ---
class DataPipeline:
    """Orchestrates the data extraction, transformation, validation, and loading."""

    def __init__(self):
        """Initializes the pipeline components using loaded configuration."""
        log.info("Initializing Data Pipeline components...")
        try:
            self.scanner = MetadataScanner(
                root_scan_path=config.config.R_SCHIJF_PAD,
                temp_dir=config.config.TEMP_EXTRACTION_DIR
            )
            self.validator = MetadataValidator()
            self.transformer = CkanTransformer(
                ckan_mapping=config.config.CKAN_MAPPING
            )
            self.uploader = CkanUploader(
                ckan_api_url=str(config.config.CKAN_API_URL), # Ensure string
                ckan_api_key=config.config.CKAN_API_KEY
            )
        except Exception as e:
             log.exception("CRITICAL: Failed to initialize pipeline components.")
             raise RuntimeError("Pipeline component initialization failed.") from e

    def run(self) -> None:
        """Executes the full data pipeline."""
        log.info("="*20 + " Starting Data Pipeline Run " + "="*20)

        if not self.uploader.is_connected():
            log.critical("CKAN connection failed during initialization. Pipeline cannot continue.")
            return

        try:
            # --- 1. Scan and Extract ---
            scan_path = config.config.R_SCHIJF_PAD
            log.info(f"Starting scan of directory: {scan_path}")
            extracted_metadata: List[Dict[str, Any]] = self.scanner.scan()

            if not extracted_metadata:
                log.info("Scan complete: No processable items found.")
                return

            log.info(f"Scan complete: Found {len(extracted_metadata)} initial items.")

            # --- 2. Validate ---
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

            if validation_issue_count > 0:
                 log.warning(f"Validation complete: Found issues in {validation_issue_count} item(s).")
            else:
                 log.info("Validation complete: No issues found.")

            # --- 3. Load and Publish ---
            log.info(f"Starting upload process for {len(items_to_process)} items...") # <<< MODIFIED LINE (Removed host)
            try:
                # Parse the URL string to safely get the hostname for logging
                ckan_url_str = config.config.CKAN_API_URL or ""
                ckan_url_parsed = urlparse(ckan_url_str)
                ckan_host = ckan_url_parsed.hostname # Use .hostname
                if ckan_host:
                     log.info(f"... Target CKAN instance: {ckan_host}") # <<< MODIFIED LINE (Log host separately)
                else:
                     log.warning(f"Could not determine hostname from CKAN URL: {ckan_url_str}")
            except Exception as parse_err:
                log.warning(f"Could not parse CKAN URL host: {parse_err}")
                # Continue without logging the host

            # Call the uploader
            resource_count, dataset_count = self.uploader.process_list(
                metadata_list=items_to_process,
                transformer=self.transformer,
                dataset_field_map=config.config.CKAN_DATASET_FIELD_MAP,
                resource_field_map=config.config.CKAN_RESOURCE_FIELD_MAP
            )

            log.info(f"Upload process complete. Processed {resource_count} resources across {dataset_count} unique datasets.")
            log.info("="*20 + " Data Pipeline Run Finished Successfully " + "="*20)

        except Exception as e:
            log.exception("CRITICAL: An unexpected error occurred during pipeline execution:")
            log.info("="*20 + " Data Pipeline Run Failed " + "="*20)


# --- Script Entry Point ---
if __name__ == "__main__":
    # Use a try-except block to catch initialization errors
    pipeline_instance = None
    try:
        pipeline_instance = DataPipeline()
    except (ValueError, RuntimeError, ImportError) as init_err:
         err_msg = f"CRITICAL [main]: Pipeline initialization failed: {init_err}"
         # Logging might not be set up, print to stderr
         import sys
         print(err_msg, file=sys.stderr)
         # Try logging as well, in case basic logging was set up
         logging.getLogger().critical(err_msg)
         sys.exit(1)
    except Exception as e:
         err_msg = f"CRITICAL [main]: Unexpected error during pipeline initialization: {e}"
         import sys
         print(err_msg, file=sys.stderr)
         logging.getLogger().critical(err_msg, exc_info=True)
         sys.exit(1)

    # If initialization succeeded, run the pipeline
    if pipeline_instance:
        try:
            pipeline_instance.run()
            # Exit with 0 if run completes (even if errors were logged during run)
            import sys
            sys.exit(0)
        except Exception as e:
            # Catch unexpected errors during run() itself
            err_msg = f"CRITICAL [main]: Unhandled exception during pipeline execution: {e}"
            import sys
            print(err_msg, file=sys.stderr)
            # Log with traceback if logger exists
            logging.getLogger("DataPipeline").exception(err_msg)
            sys.exit(1) # Exit with error status if run() fails catastrophically