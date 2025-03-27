# main.py
import logging
from typing import List, Dict, Any # Keep basic types if needed

# --- Module Imports ---
# Import the singleton config object and utility function
try:
    import config # Import the module to access the singleton 'config' instance
    import utils
except (ImportError, ValueError, RuntimeError) as config_err:
    # Catch errors during config loading/validation (ImportError/ValueError/RuntimeError)
    # Log directly to stderr as logging might not be set up.
    import sys
    print(f"CRITICAL [main]: Failed to load configuration or utils: {config_err}", file=sys.stderr)
    sys.exit(1) # Exit if config fails

# Import the necessary classes from refactored modules
try:
    from extract_transform import MetadataScanner, CkanTransformer, MetadataValidator
    from load_publish import CkanUploader
except ImportError as import_err:
    # Log directly to stderr if class imports fail
    import sys
    print(f"CRITICAL [main]: Failed to import necessary classes: {import_err}", file=sys.stderr)
    print("Ensure extract_transform.py and load_publish.py are correctly refactored and accessible.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
# Setup logging using the configuration loaded by the config module
# It's crucial that config loading succeeded before this point.
try:
    log = utils.setup_logging(
        log_file=config.config.LOG_FILE, # Access attributes via the 'config' instance
        level=config.config.LOG_LEVEL,
        logger_name="DataPipeline" # Give the main pipeline logger a name
    )
except Exception as log_err:
     # Fallback if logging setup itself fails
     import sys
     print(f"CRITICAL [main]: Failed to set up logging: {log_err}", file=sys.stderr)
     # Continue execution? Or exit? Let's try to continue with basic logging.
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
            # Access configuration via the singleton instance 'config.config'
            self.scanner = MetadataScanner(
                root_scan_path=config.config.R_SCHIJF_PAD,
                temp_dir=config.config.TEMP_EXTRACTION_DIR
                # Removed max_workers based on extract_transform simplification
            )
            self.validator = MetadataValidator()
            self.transformer = CkanTransformer(
                ckan_mapping=config.config.CKAN_MAPPING # Use the general mapping
            )
            self.uploader = CkanUploader(
                ckan_api_url=str(config.config.CKAN_API_URL), # Ensure string for ckanapi
                ckan_api_key=config.config.CKAN_API_KEY
            )
        except Exception as e:
             log.exception("CRITICAL: Failed to initialize pipeline components.")
             # Reraise or handle appropriately - pipeline cannot run
             raise RuntimeError("Pipeline component initialization failed.") from e

    def run(self) -> None:
        """Executes the full data pipeline."""
        log.info("="*20 + " Starting Data Pipeline Run " + "="*20)

        # --- Check CKAN Connection (established during CkanUploader init) ---
        if not self.uploader.is_connected():
            log.critical("CKAN connection failed during initialization. Pipeline cannot continue.")
            return # Stop if no CKAN connection

        try:
            # --- 1. Scan and Extract ---
            scan_path = config.config.R_SCHIJF_PAD # For logging
            log.info(f"Starting scan of directory: {scan_path}")
            extracted_metadata: List[Dict[str, Any]] = self.scanner.scan()

            if not extracted_metadata:
                log.info("Scan complete: No processable items found.")
                return # Exit cleanly if nothing to process

            log.info(f"Scan complete: Found {len(extracted_metadata)} initial items.")

            # --- 2. Validate (Optional but Recommended) ---
            log.info("Starting metadata validation...")
            items_to_process: List[Dict[str, Any]] = []
            validation_issue_count = 0
            for item_meta in extracted_metadata:
                rel_path = item_meta.get('relative_path', item_meta.get('bestandspad', 'Unknown'))
                issues = self.validator.validate(item_meta)
                if issues:
                     validation_issue_count += 1
                     log.warning(f"Validation issues for '{rel_path}': {issues}")
                     # Policy: Log issues but still attempt to process the item? Or skip?
                     # Let's process all items for now and let CKAN potentially reject them.
                     # Add validation info to metadata if needed downstream?
                     # item_meta['_validation_issues'] = issues # Example
                items_to_process.append(item_meta)

            if validation_issue_count > 0:
                 log.warning(f"Validation complete: Found issues in {validation_issue_count} item(s).")
            else:
                 log.info("Validation complete: No issues found.")


            # --- 3. Load and Publish ---
            log.info(f"Starting upload process for {len(items_to_process)} items to CKAN: {config.config.CKAN_API_URL.host}...")
            resource_count, dataset_count = self.uploader.process_list(
                metadata_list=items_to_process,
                transformer=self.transformer,
                # Pass the specific dataset/resource maps from config
                dataset_field_map=config.config.CKAN_DATASET_FIELD_MAP,
                resource_field_map=config.config.CKAN_RESOURCE_FIELD_MAP
            )

            log.info(f"Upload process complete. Processed {resource_count} resources across {dataset_count} unique datasets.")
            log.info("="*20 + " Data Pipeline Run Finished Successfully " + "="*20)

        except Exception as e:
            # Catch unexpected errors during the main pipeline execution
            log.exception("CRITICAL: An unexpected error occurred during pipeline execution:")
            # Optionally add notifications here
            # utils.send_notification("Pipeline Failure", f"Critical error: {e}")
            log.info("="*20 + " Data Pipeline Run Failed " + "="*20)


# --- Script Entry Point ---
if __name__ == "__main__":
    try:
        pipeline = DataPipeline()
        pipeline.run()
    except (ValueError, RuntimeError, ImportError) as init_err:
         # Catch critical errors during config loading or component initialization
         # Logging might not be fully set up, so print to stderr as well
         err_msg = f"CRITICAL [main]: Pipeline initialization failed: {init_err}"
         print(err_msg, file=sys.stderr)
         if log: log.critical(err_msg) # Log if logger exists
         sys.exit(1) # Exit with error status
    except Exception as e:
         # Catch any other unexpected errors at the top level
         err_msg = f"CRITICAL [main]: Unhandled exception during pipeline execution: {e}"
         print(err_msg, file=sys.stderr)
         if log: log.exception(err_msg) # Log with traceback if logger exists
         sys.exit(1)