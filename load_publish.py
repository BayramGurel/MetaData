# load_publish.py
import logging
from typing import Dict, Any, Optional, List, Tuple, Set, NamedTuple
import re # For slugify

# --- Dependency Imports & Fallbacks ---
try:
    from ckanapi import RemoteCKAN, NotFound, NotAuthorized, CKANAPIError, ValidationError
except ImportError:
    logging.critical("CKANAPI library not found. Install: pip install ckanapi")
    # Dummy classes for basic loading
    class CKANException(Exception): pass
    class NotFound(CKANException): pass
    class NotAuthorized(CKANException): pass
    class CKANAPIError(CKANException): pass
    class ValidationError(CKANAPIError): error_dict: dict = {}
    class RemoteCKAN: __init__ = lambda *a, **kw: (_ for _ in ()).throw(ImportError("CKANAPI missing")) # type: ignore

from pathlib import Path
import time
import json

try:
    # Import the transformer class definition for type hinting
    from extract_transform import CkanTransformer
except ImportError:
    logging.warning("CkanTransformer not imported. Type hinting incomplete.")
    class CkanTransformer: # Dummy for type hint
        def __init__(self, *args, **kwargs): pass
        def transform(self, metadata: Dict[str, Any]) -> Dict[str, Any]: return metadata

# --- Add import for config singleton ---
try:
    import config # Access the singleton instance via config.config
except ImportError:
     logging.critical("config.py not found. Cannot proceed.")
     raise
# --- End Dependency Imports ---

log = logging.getLogger(__name__) # Logger specific to this module

# --- Helper Data Structure ---
class UploadInfo(NamedTuple):
    """Structure to hold prepared info for uploading a single item."""
    success: bool
    item_meta: Dict[str, Any] # Original metadata for context
    transformed_meta: Dict[str, Any]
    target_org_name: Optional[str]
    target_dataset_slug: str
    dataset_payload: Dict[str, Any]
    resource_payload: Optional[Dict[str, Any]] # None for directory items
    file_path: Optional[Path] # None for directory items
    file_hash: Optional[str] # None for directory items or hash failure
    error_message: Optional[str] = None

# --- Main Class ---
class CkanUploader:
    """
    Handles CKAN connection, caching, and publishing datasets/resources.
    Operates on metadata dictionaries provided by the extraction/transformation stage.
    """

    DEFAULT_RETRIES = 0 # Default to no retries for API calls
    DEFAULT_RETRY_DELAY = 3
    FALLBACK_ORG_NAME = "unspecified-organization" # Hardcoded fallback org slug

    def __init__(self, ckan_api_url: str, ckan_api_key: Optional[str]):
        """Initializes connection details, caches, and connects to CKAN."""
        self.api_url = ckan_api_url
        self.api_key = ckan_api_key
        self.ckan: Optional[RemoteCKAN] = None
        self.org_cache: Dict[str, Optional[str]] = {} # name -> id | None (if failed)
        self.dataset_cache: Dict[str, Optional[str]] = {} # slug -> id | None (if failed)
        self.processed_resource_paths: Set[str] = set() # Store resolved paths processed

        self._connect() # Attempt connection on initialization

    def _connect(self) -> None:
        """Establishes and tests the CKAN connection."""
        log.info(f"Connecting to CKAN: {self.api_url}")
        try:
            self.ckan = RemoteCKAN(self.api_url, apikey=self.api_key)
            self.ckan.action.status_show() # Use basic action for connection test
            log.info("CKAN connection successful.")
        except (NotAuthorized, ConnectionError, CKANAPIError) as e:
            log.error(f"CKAN connection failed: {e}")
            self.ckan = None
        except Exception as e:
            log.exception(f"Unexpected CKAN connection error: {e}")
            self.ckan = None

    def is_connected(self) -> bool:
        """Check if successfully connected to CKAN."""
        return self.ckan is not None

    def _execute_action(self, action: str, data_dict: Optional[Dict[str, Any]] = None,
                        log_prefix: str = "", retries: int = DEFAULT_RETRIES,
                        delay: int = DEFAULT_RETRY_DELAY) -> Any:
        """Internal helper to execute CKAN actions with error handling."""
        if not self.ckan: raise ConnectionError("CKAN not connected.")
        params = data_dict or {}
        last_exception: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                log.debug(f"{log_prefix}CKAN Action '{action}' attempt {attempt+1}/{retries+1}")
                action_func = getattr(self.ckan.action, action)
                return action_func(**params)
            except AttributeError: # Action name invalid in ckanapi
                 log.error(f"{log_prefix}Invalid action name '{action}' in ckanapi library.")
                 raise # Fatal code error
            except NotFound: return None # API reported 'not found'
            except (NotAuthorized, ValidationError) as e: raise e # Fail immediately
            except CKANAPIError as e: last_exception = e; log.warning(f"{log_prefix}CKAN API Error ({action}): {e}")
            except Exception as e: last_exception = e; log.warning(f"{log_prefix}Unexpected Error ({action}): {e}")
            if attempt < retries: time.sleep(delay)
            else: log.error(f"{log_prefix}Action '{action}' failed after retries."); raise last_exception or CKANAPIError("Action failed")

    # --- Org/Dataset/Resource Methods ---

    def get_or_create_organization(self, org_name: str, org_title: Optional[str] = None, description: Optional[str] = None) -> Optional[str]:
        """Gets or creates an organization, caches result. Returns ID or None."""
        log_prefix = f"[Org: {org_name}] "
        if not org_name: return None
        if org_name in self.org_cache: return self.org_cache[org_name]

        org_id: Optional[str] = None
        try:
            org = self._execute_action("organization_show", {"id": org_name}, log_prefix)
            if org: org_id = org.get("id") # Found existing
            else: # Not found, try create
                if not self.api_key: log.error(f"{log_prefix}Cannot create: API key missing."); raise NotAuthorized()
                log.info(f"{log_prefix}Not found, creating...")
                payload = {"name": org_name, "title": org_title or org_name.replace('-',' ').title(),
                           "description": description or f"Org: {org_title or org_name}", "state": "active"}
                new_org = self._execute_action("organization_create", payload, log_prefix)
                if new_org: org_id = new_org.get("id") # Created
        except Exception as e: log.error(f"{log_prefix}Error ensuring org exists: {e}")

        self.org_cache[org_name] = org_id # Cache result (ID or None)
        log.info(f"{log_prefix}{'Found' if org else ('Created' if org_id else 'Failed')} (ID: {org_id})")
        return org_id

    def get_or_create_dataset(self, dataset_slug: str, org_id: Optional[str], dataset_payload: Dict[str, Any]) -> Optional[str]:
        """Gets/Creates/Updates a dataset, caches result. Returns ID or None."""
        log_prefix = f"[Dataset: {dataset_slug}] "
        if not dataset_slug: return None
        if dataset_slug in self.dataset_cache: return self.dataset_cache[dataset_slug]

        dataset_id: Optional[str] = None
        try:
            existing_ds = self._execute_action("package_show", {"id": dataset_slug}, log_prefix)
            payload = dataset_payload.copy(); payload['name'] = dataset_slug

            if existing_ds: # Update path
                 existing_id = existing_ds.get("id")
                 if not existing_id: raise ValueError("Existing dataset lacks ID")
                 log.info(f"{log_prefix}Found (ID: {existing_id}), updating...")
                 if org_id and existing_ds.get('owner_org') != org_id: raise ValueError("Organization mismatch") # Prevent wrong update
                 payload['id'] = existing_id; payload['owner_org'] = org_id
                 updated_ds = self._execute_action("package_update", payload, log_prefix)
                 if updated_ds: dataset_id = updated_ds.get("id")
                 if dataset_id: log.info(f"{log_prefix}Updated (ID: {dataset_id})")
                 else: log.error(f"{log_prefix}Update failed.")

            else: # Create path
                 log.info(f"{log_prefix}Not found, creating...")
                 if not self.api_key: raise NotAuthorized("API key missing for create")
                 if org_id: payload['owner_org'] = org_id
                 else: # If org_id is None, CKAN might require it - let the API call fail if so
                     payload.pop('owner_org', None) # Don't send owner_org=None explicitly
                     log.warning(f"{log_prefix}Attempting create without org ID (may fail if required by CKAN).")
                 if not payload.get('title'): payload['title'] = dataset_slug.replace('-',' ').title()
                 new_ds = self._execute_action("package_create", payload, log_prefix)
                 if new_ds: dataset_id = new_ds.get("id")
                 if dataset_id: log.info(f"{log_prefix}Created (ID: {dataset_id})")
                 else: log.error(f"{log_prefix}Create failed.")

        except (NotAuthorized, ValidationError, CKANAPIError, ConnectionError, ValueError, Exception) as e:
             # --- SIMPLIFIED ERROR HANDLING ---
             # Log the actual error encountered during create/update/show
             log.error(f"{log_prefix}Error ensuring dataset exists: {e}")
             # dataset_id remains None

        self.dataset_cache[dataset_slug] = dataset_id # Cache final result (ID or None)
        return dataset_id

    def _resource_needs_update(self, res_id: str, current_hash: Optional[str], payload: Dict[str, Any], log_prefix: str) -> bool:
        """Checks if resource needs update (hash or metadata)."""
        try:
            existing = self._execute_action("resource_show", {"id": res_id}, log_prefix)
            if not existing: return True
            if current_hash and existing.get("hash") != current_hash: return True
            for k, v in payload.items():
                 if k in ['id', 'package_id', 'upload','hash'] or k.startswith('_'): continue # Exclude hash here too
                 if existing.get(k) != v: return True
            log.info(f"{log_prefix}Resource appears up-to-date.")
            return False
        except Exception as e: log.error(f"{log_prefix}Error checking resource: {e}. Assuming update needed."); return True

    def upsert_resource(self, dataset_id: str, resource_payload: Dict[str, Any],
                        file_path: Path, file_hash: Optional[str]) -> Optional[Dict[str, Any]]:
        """Creates/Updates resource, uploads file. Returns resource dict or None."""
        res_name = resource_payload.get("name"); log_prefix = f"[DS:{dataset_id}][Res:{res_name}] "
        if not res_name: log.error(f"{log_prefix}Payload missing 'name'."); return None
        if not file_path.is_file(): log.error(f"{log_prefix}File not found: {file_path}."); return None

        abs_path_str = str(file_path.resolve())
        if abs_path_str in self.processed_resource_paths: log.info(f"{log_prefix}Already processed: {abs_path_str}. Skip."); return None

        upserted_res: Optional[Dict[str, Any]] = None
        try:
            existing_res = None; res_id = None
            search = self._execute_action("resource_search", {"query": f'name:"{res_name}" AND package_id:"{dataset_id}"'}, log_prefix)
            if search and search.get('count', 0) > 0: existing_res = search['results'][0]; res_id = existing_res.get("id")

            payload = resource_payload.copy(); payload['package_id'] = dataset_id
            action = "resource_create"; payload.pop('id', None)
            needs_api_call = True

            if res_id:
                 payload['id'] = res_id; action = "resource_update"
                 if not self._resource_needs_update(res_id, file_hash, payload, log_prefix):
                      needs_api_call = False; upserted_res = existing_res
            elif existing_res: log.error(f"{log_prefix}Existing resource lacks ID."); return None # Should not happen

            if needs_api_call:
                 verb = "Updating" if action == "resource_update" else "Creating"
                 log.info(f"{log_prefix}{verb} resource...")
                 with file_path.open('rb') as file_obj:
                      payload['upload'] = file_obj
                      if file_hash and 'hash' in payload: payload['hash'] = file_hash # Add hash if mapped
                      upserted_res = self._execute_action(action, payload, log_prefix)

            if upserted_res and upserted_res.get("id"):
                 if needs_api_call: log.info(f"{log_prefix}Success (ID: {upserted_res['id']}).")
                 self.processed_resource_paths.add(abs_path_str); return upserted_res
            else:
                 if needs_api_call: log.error(f"{log_prefix}{action.replace('_',' ')} failed.")
                 return None
        except Exception as e: log.error(f"{log_prefix}Failed to upsert: {e}"); return None


    # --- Main Processing Logic Helpers ---

    def _generate_slug(self, name: str, max_length: int = 100) -> str:
        """Generates CKAN-friendly slug."""
        if not name: return "unnamed"
        slug = str(name).lower().strip(); slug = re.sub(r'[\s\._]+', '-', slug); slug = re.sub(r'[^\w-]', '', slug); slug = re.sub(r'-{2,}', '-', slug); slug = slug.strip('-')[:max_length]; return slug or "unnamed"

    def _determine_dataset_slug(self, item_meta: Dict[str, Any]) -> str:
        """
        Determines target dataset slug based on item metadata.
        Strategy: Directory name for directories. Parent directory name for files in subdirs.
                  File stem for files in root. Fallback to "default-dataset".
        """
        rel_path = item_meta.get("relative_path", ""); item_type = item_meta.get("bestandstype", "")
        base = ""; parts = Path(rel_path).parts
        if item_type == 'directory': base = parts[-1] if parts else ""
        elif len(parts) > 1: base = parts[-2] # File in subdir -> parent dir
        elif parts: base = Path(parts[0]).stem # File in root -> file stem
        else: base = Path(item_meta.get("bestandspad", "")).stem # Fallback
        return self._generate_slug(base) or "default-dataset"

    def _prepare_ckan_payload(self, transformed_metadata: Dict[str, Any], field_map: Dict[str, str]) -> Dict[str, Any]:
        """Filters transformed metadata based on field map and handles list formats."""
        payload = {t: transformed_metadata[s] for s, t in field_map.items() if s in transformed_metadata}
        # Ensure tags format: list of {'name': 'tag'}
        if 'tags' in payload:
            tags_val = payload['tags']
            if isinstance(tags_val, list): payload['tags'] = [{'name': str(t).strip()} for t in tags_val if str(t).strip()]
            elif isinstance(tags_val, str): payload['tags'] = [{'name': t.strip()} for t in tags_val.split(',') if t.strip()]
            else: del payload['tags']; log.warning("Invalid tags format removed.")
        # Ensure groups format: list of {'id': 'group_id'}
        if 'groups' in payload and isinstance(payload['groups'], list):
            payload['groups'] = [{'id': str(g).strip()} for g in payload['groups'] if str(g).strip()]
        return payload

    def _prepare_upload_info(self, item_meta: Dict[str, Any], transformer: CkanTransformer,
                              ds_map: Dict[str, str], res_map: Dict[str, str]) -> UploadInfo:
        """Transforms metadata, determines targets, prepares payloads for one item."""
        try:
            transformed = transformer.transform(item_meta)
            org_name = transformed.get("owner_org") # Relies on transformer mapping potential_org_id
            dataset_slug = self._determine_dataset_slug(item_meta)
            ds_payload = self._prepare_ckan_payload(transformed, ds_map)
            res_payload = None; f_path = None; f_hash = None

            # Only prepare resource info if it's not a directory
            if item_meta.get("bestandstype") != 'directory':
                 res_payload = self._prepare_ckan_payload(transformed, res_map)
                 # Ensure resource name exists, fallback to filename
                 res_name_base = item_meta.get("bestandsnaam", Path(item_meta.get("bestandspad", "")).name)
                 if not res_payload.get('name'): res_payload['name'] = res_name_base
                 # Optionally slugify resource name if needed by CKAN/plugins
                 # res_payload['name'] = self._generate_slug(res_payload['name'])
                 f_path = Path(item_meta.get("bestandspad", ""))
                 f_hash = transformed.get('hash')

            return UploadInfo(True, item_meta, transformed, org_name, dataset_slug, ds_payload, res_payload, f_path, f_hash)
        except Exception as e:
            log.exception(f"Prep error for {item_meta.get('bestandspad')}: {e}")
            return UploadInfo(False, item_meta, {}, None, "", {}, None, None, None, str(e))

    # --- Main Processing Method ---

    def process_list(self, metadata_list: List[Dict[str, Any]], transformer: CkanTransformer,
                     dataset_field_map: Dict[str, str], resource_field_map: Dict[str, str]) -> Tuple[int, int]:
        """Processes list of metadata: prepares, ensures orgs/datasets, upserts resources."""
        if not self.is_connected(): log.error("CKAN not connected."); return 0, 0
        res_count, ds_ids = 0, set()
        log.info(f"Processing {len(metadata_list)} items for CKAN...")

        for item_meta in metadata_list:
            prep_info = self._prepare_upload_info(item_meta, transformer, dataset_field_map, resource_field_map)
            if not prep_info.success:
                 log.error(f"Skipping item due to prep error: {item_meta.get('bestandspad')} ({prep_info.error_message})")
                 continue

            log_prefix = f"[Item: {item_meta.get('relative_path', '?')}] "
            log.debug(f"{log_prefix}Prep: Org='{prep_info.target_org_name}', DS='{prep_info.target_dataset_slug}'")

            # --- Ensure Org ---
            org_name_to_use = prep_info.target_org_name or self.FALLBACK_ORG_NAME
            org_id: Optional[str] = None
            if org_name_to_use:
                 org_id = self.get_or_create_organization(org_name_to_use) # Use simpler call signature if title/desc defaults are ok
                 if not org_id: log.warning(f"{log_prefix}Failed org '{org_name_to_use}'. Dataset creation may fail.")
            else: log.critical(f"{log_prefix}No org name available (specific or fallback). Dataset creation WILL fail.") # Should not happen if FALLBACK is set

            # --- Ensure Dataset ---
            dataset_id = self.get_or_create_dataset(prep_info.target_dataset_slug, org_id, prep_info.dataset_payload)
            if not dataset_id: log.warning(f"{log_prefix}Failed dataset '{prep_info.target_dataset_slug}'. Skipping resource."); continue
            ds_ids.add(dataset_id)

            # --- Upsert Resource (if file) ---
            if prep_info.resource_payload and prep_info.file_path:
                resource = self.upsert_resource(dataset_id, prep_info.resource_payload, prep_info.file_path, prep_info.file_hash)
                if resource: res_count += 1
                else: log.warning(f"{log_prefix}Failed resource '{prep_info.resource_payload.get('name')}' in ds '{prep_info.target_dataset_slug}'.")
            elif item_meta.get("bestandstype") != 'directory': log.warning(f"{log_prefix}File item missing resource info. Skip.")

        log.info(f"CKAN processing summary: {res_count} resources processed, {len(ds_ids)} datasets processed.")
        return res_count, len(ds_ids)


# --- Example Usage --- (Simplified setup)
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(name)s: %(message)s')
    try:
        # Assumes config.py provides 'config' instance
        # Use str() for URL just in case config returns HttpUrl type
        if not config.config.CKAN_API_URL: raise ValueError("CKAN_API_URL missing")

        log.info("--- Starting CkanUploader Example ---")
        # Minimal dummy data
        dummy_meta = [
            {'bestandspad': 'test/dira', 'bestandstype': 'directory', 'relative_path': 'dira', 'metadata': {}},
            {'bestandspad': 'test/dira/f1.txt', 'bestandstype': 'TXT', 'relative_path': 'dira/f1.txt', 'metadata': {'hash': 'h1'}},
            {'bestandspad': 'test/dira/f2.pdf', 'bestandstype': 'PDF', 'relative_path': 'dira/f2.pdf', 'metadata': {'hash': 'h2', 'titel': 'PDF Two'}},
            {'bestandspad': 'test/other.zip', 'bestandstype': 'ZIP', 'relative_path': 'other.zip', 'metadata': {'potential_organization_id': 'zip-org'}},
        ]
        # Basic file creation for testing upsert
        for item in dummy_meta:
             p = Path(item['bestandspad']); p.parent.mkdir(parents=True, exist_ok=True)
             if item['bestandstype'] != 'directory' and not p.exists(): p.touch()

        transformer = CkanTransformer(config.config.CKAN_MAPPING)
        uploader = CkanUploader(str(config.config.CKAN_API_URL), config.config.CKAN_API_KEY)

        if uploader.is_connected():
            res_c, ds_c = uploader.process_list(
                dummy_meta, transformer,
                config.config.CKAN_DATASET_FIELD_MAP,
                config.config.CKAN_RESOURCE_FIELD_MAP
            )
            log.info(f"Example finished. Resources: {res_c}, Datasets: {ds_c}")
        else: log.error("Example failed: No CKAN connection.")

        # Cleanup (optional)
        # import shutil; shutil.rmtree("test", ignore_errors=True)

    except ImportError: log.critical("Failed to import config.py for example.")
    except Exception as e: log.exception(f"Error in example execution: {e}")