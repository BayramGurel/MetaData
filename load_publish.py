# load_publish.py
import logging
from typing import Dict, Any, Optional, List, Tuple, Set, NamedTuple
import re # For slugify

# --- Dependency Imports & Fallbacks ---
try:
    # Ensure ckanapi is installed: pip install ckanapi
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
    """Handles CKAN connection, caching, and data publishing."""

    DEFAULT_RETRIES = 0 # Default to no retries for simplicity
    DEFAULT_RETRY_DELAY = 3

    def __init__(self, ckan_api_url: str, ckan_api_key: Optional[str]):
        self.api_url = ckan_api_url
        self.api_key = ckan_api_key
        self.ckan: Optional[RemoteCKAN] = None
        self.org_cache: Dict[str, Optional[str]] = {} # name -> id | None
        self.dataset_cache: Dict[str, Optional[str]] = {} # slug -> id | None
        self.processed_resource_paths: Set[str] = set() # Store resolved paths processed in this run

        self._connect()

    def _connect(self) -> None:
        """Establishes and tests CKAN connection."""
        log.info(f"Connecting to CKAN: {self.api_url}")
        try:
            self.ckan = RemoteCKAN(self.api_url, apikey=self.api_key)
            # --- MODIFIED LINE: Changed connection test action ---
            self.ckan.action.status_show() # Test connection using a very basic action
            log.info("CKAN connection successful.")
        except (NotAuthorized, ConnectionError, CKANAPIError) as e:
            log.error(f"CKAN connection failed: {e}")
            self.ckan = None
        except Exception as e:
            log.exception(f"Unexpected CKAN connection error: {e}")
            self.ckan = None

    def is_connected(self) -> bool:
        return self.ckan is not None

    def _execute_action(self, action: str, data_dict: Optional[Dict[str, Any]] = None,
                        log_prefix: str = "", retries: int = DEFAULT_RETRIES,
                        delay: int = DEFAULT_RETRY_DELAY) -> Any:
        """Executes CKAN action with error handling and optional retries."""
        if not self.ckan: raise ConnectionError("CKAN not connected.")

        params = data_dict or {}
        last_exception: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                log.debug(f"{log_prefix}CKAN Action '{action}' attempt {attempt+1}/{retries+1}")
                # Use dynamic attribute access for actions
                action_func = getattr(self.ckan.action, action)
                return action_func(**params)
            except AttributeError: # Catch if action name itself is invalid for the library object
                 log.error(f"{log_prefix}CKAN action name '{action}' not found in ckanapi library object.")
                 raise # Fail immediately if action name is wrong in code
            except NotFound: return None
            except (NotAuthorized, ValidationError) as e: raise e # Fail immediately
            except CKANAPIError as e: # Includes 400 Bad Request, 500 Server Error etc.
                log.warning(f"{log_prefix}CKAN API Error ({action}): {e}")
                last_exception = e
            except Exception as e: # Other errors like network timeouts etc.
                log.warning(f"{log_prefix}Unexpected Error ({action}): {e}", exc_info=log.level <= logging.DEBUG)
                last_exception = e

            if attempt < retries: time.sleep(delay)
            else: # Last attempt failed
                 log.error(f"{log_prefix}CKAN action '{action}' failed after {retries+1} attempts.")
                 if last_exception: raise last_exception
                 else: raise CKANAPIError(f"Action '{action}' failed without specific exception captured.")

    # --- Org/Dataset/Resource Methods ---

    def get_or_create_organization(self, org_name: str, org_title: Optional[str] = None, description: Optional[str] = None) -> Optional[str]:
        """Gets or creates an organization, caches result. Returns ID or None."""
        log_prefix = f"[Org: {org_name}] "
        if not org_name: return None
        if org_name in self.org_cache: return self.org_cache[org_name] # Return cached result (ID or None)

        org_id: Optional[str] = None # Initialize id
        try:
            org_data = self._execute_action("organization_show", {"id": org_name}, log_prefix)
            if org_data and (org_id := org_data.get("id")):
                 log.info(f"{log_prefix}Found (ID: {org_id})")
            else: # Not found, try creating
                if not self.api_key: log.error(f"{log_prefix}Cannot create: API key missing."); raise NotAuthorized()
                log.info(f"{log_prefix}Not found, creating...")
                payload = {"name": org_name, "title": org_title or org_name.replace('-', ' ').title(),
                           "description": description or f"Org: {org_title or org_name}", "state": "active"}
                new_org = self._execute_action("organization_create", payload, log_prefix)
                if new_org and (org_id := new_org.get("id")): log.info(f"{log_prefix}Created (ID: {org_id})")
                else: log.error(f"{log_prefix}Create failed or returned no ID.") # org_id remains None

        except (NotAuthorized, ValidationError, CKANAPIError, ConnectionError, Exception) as e:
             log.error(f"{log_prefix}Error ensuring org exists: {e}")
             # org_id remains None

        self.org_cache[org_name] = org_id # Cache the final result (ID or None)
        return org_id

    def get_or_create_dataset(self, dataset_slug: str, org_id: Optional[str], dataset_payload: Dict[str, Any]) -> Optional[str]:
        """Gets/Creates/Updates a dataset, caches result. Returns ID or None."""
        log_prefix = f"[Dataset: {dataset_slug}] "
        if not dataset_slug: return None
        if dataset_slug in self.dataset_cache: return self.dataset_cache[dataset_slug]

        dataset_id: Optional[str] = None # Initialize id
        existing_ds = None # <<< --- INITIALIZE HERE TO FIX UnboundLocalError ---
        try:
            existing_ds = self._execute_action("package_show", {"id": dataset_slug}, log_prefix)
            payload = dataset_payload.copy(); payload['name'] = dataset_slug

            if existing_ds: # Update path
                 existing_id = existing_ds.get("id")
                 if not existing_id: log.error(f"{log_prefix}Show OK but no ID?"); raise ValueError("Existing dataset lacks ID")
                 log.info(f"{log_prefix}Found (ID: {existing_id}), updating...")
                 # Org check
                 if org_id and existing_ds.get('owner_org') != org_id:
                      log.warning(f"{log_prefix}Org mismatch! Skip update."); raise ValueError("Organization mismatch") # Raise to prevent wrong update
                 payload['id'] = existing_id; payload['owner_org'] = org_id
                 updated_ds = self._execute_action("package_update", payload, log_prefix)
                 if updated_ds and (dataset_id := updated_ds.get("id")): log.info(f"{log_prefix}Updated (ID: {dataset_id})")
                 else: log.error(f"{log_prefix}Update failed or returned no ID.")

            else: # Create path
                 log.info(f"{log_prefix}Not found, creating...")
                 if not self.api_key: log.error(f"{log_prefix}Cannot create: API key missing."); raise NotAuthorized()
                 if org_id: payload['owner_org'] = org_id
                 elif 'owner_org' in payload: del payload['owner_org']
                 if not payload.get('title'): payload['title'] = dataset_slug.replace('-', ' ').title()
                 new_ds = self._execute_action("package_create", payload, log_prefix)
                 if new_ds and (dataset_id := new_ds.get("id")): log.info(f"{log_prefix}Created (ID: {dataset_id})")
                 else: log.error(f"{log_prefix}Create failed or returned no ID.")

        except (NotAuthorized, ValidationError, CKANAPIError, ConnectionError, Exception) as e:
             # Handle specific race condition on create only
             if not existing_ds and isinstance(e, ValidationError) and 'name' in getattr(e, 'error_dict', {}):
                  if any('already exists' in msg.lower() for msg in e.error_dict['name']):
                       log.warning(f"{log_prefix}Create conflict (race condition?), trying fetch again.")
                       try:
                           refetched = self._execute_action("package_show", {"id": dataset_slug}, log_prefix)
                           if refetched and (dataset_id := refetched.get("id")): # Assign to dataset_id if successful
                                log.info(f"{log_prefix}Found dataset ID {dataset_id} after conflict.")
                           else: log.error(f"{log_prefix}Refetch after conflict failed.")
                       except Exception as fetch_e: log.error(f"{log_prefix}Refetch after conflict failed: {fetch_e}")
             # Log other errors if race condition didn't apply or refetch failed
             if not dataset_id: # Only log error if we didn't succeed in race condition recovery
                  log.error(f"{log_prefix}Error ensuring dataset exists: {e}")
             # dataset_id remains None if any error occurred and wasn't recovered

        self.dataset_cache[dataset_slug] = dataset_id # Cache final result (ID or None)
        return dataset_id

    def _resource_needs_update(self, res_id: str, current_hash: Optional[str], payload: Dict[str, Any], log_prefix: str) -> bool:
        """Checks if resource needs update (hash or metadata)."""
        try:
            existing = self._execute_action("resource_show", {"id": res_id}, log_prefix)
            if not existing: return True # Not found -> needs create/update

            # Check Hash
            if current_hash and existing.get("hash") != current_hash: return True

            # Check Metadata
            for k, v in payload.items():
                 if k in ['id', 'package_id', 'upload'] or k.startswith('_'): continue
                 if existing.get(k) != v: return True # Found difference

            log.info(f"{log_prefix}Resource appears up-to-date.")
            return False # No changes detected
        except Exception as e:
            log.error(f"{log_prefix}Error checking resource status: {e}. Assuming update needed.")
            return True

    def upsert_resource(self, dataset_id: str, resource_payload: Dict[str, Any],
                        file_path: Path, file_hash: Optional[str]) -> Optional[Dict[str, Any]]:
        """Creates/Updates resource, uploads file. Returns resource dict or None."""
        res_name = resource_payload.get("name")
        log_prefix = f"[DS:{dataset_id}][Res:{res_name}] "
        if not res_name: log.error(f"{log_prefix}Payload missing 'name'."); return None
        if not file_path.is_file(): log.error(f"{log_prefix}File not found: {file_path}."); return None

        abs_path_str = str(file_path.resolve())
        if abs_path_str in self.processed_resource_paths:
             log.info(f"{log_prefix}Already processed: {abs_path_str}. Skipping."); return None

        upserted_resource: Optional[Dict[str, Any]] = None # Initialize
        try:
            existing_res = None; res_id = None
            search = self._execute_action("resource_search", {"query": f'name:"{res_name}" AND package_id:"{dataset_id}"'}, log_prefix)
            if search and search.get('count', 0) > 0: existing_res = search['results'][0]; res_id = existing_res.get("id")

            payload = resource_payload.copy(); payload['package_id'] = dataset_id
            action = "resource_create"; payload.pop('id', None)
            needs_api_call = True

            if res_id: # If existing resource found by search
                 payload['id'] = res_id; action = "resource_update"
                 if not self._resource_needs_update(res_id, file_hash, payload, log_prefix):
                      needs_api_call = False; upserted_resource = existing_res # No change needed
            elif existing_res: # Found but no ID?
                 log.error(f"{log_prefix}Existing resource lacks ID. Cannot update."); return None

            if needs_api_call:
                 log_info_verb = "Updating" if action == "resource_update" else "Creating"
                 log.info(f"{log_prefix}{log_info_verb} resource...")
                 with file_path.open('rb') as file_obj:
                      payload['upload'] = file_obj
                      if file_hash and 'hash' in payload: payload['hash'] = file_hash
                      upserted_resource = self._execute_action(action, payload, log_prefix)

            # --- Final Logging and State Update ---
            if upserted_resource and upserted_resource.get("id"):
                 if needs_api_call: # Only log success if API call was made
                     log.info(f"{log_prefix}Successfully {action.replace('_',' ')}d (ID: {upserted_resource['id']}).")
                 self.processed_resource_paths.add(abs_path_str) # Mark as processed
                 return upserted_resource
            else: # Handle API call failure or no-change scenario where existing_res was None initially
                 if needs_api_call: # Only log error if API call was attempted and failed
                     log.error(f"{log_prefix}{action.replace('_',' ')} failed or returned invalid data.")
                 return None # Return None on failure or if no update was needed and resource didn't exist

        except (FileNotFoundError, PermissionError) as e: log.error(f"{log_prefix}File error: {e}"); return None
        except (NotAuthorized, ValidationError, CKANAPIError, ConnectionError, Exception) as e:
             log.error(f"{log_prefix}Failed to upsert resource: {e}"); return None


    # --- Main Processing Logic Helpers --- (No changes needed here)

    def _generate_slug(self, name: str, max_length: int = 100) -> str:
        """Generates CKAN-friendly slug."""
        if not name: return "unnamed"
        slug = str(name).lower().strip(); slug = re.sub(r'[\s\._]+', '-', slug); slug = re.sub(r'[^\w-]', '', slug); slug = re.sub(r'-{2,}', '-', slug); slug = slug.strip('-')[:max_length]; return slug or "unnamed"

    def _determine_dataset_slug(self, item_meta: Dict[str, Any]) -> str:
        """Determines target dataset slug based on item metadata (path/type)."""
        rel_path = item_meta.get("relative_path", ""); item_type = item_meta.get("bestandstype", "")
        base = ""; parts = Path(rel_path).parts
        if item_type == 'directory': base = parts[-1] if parts else ""
        elif len(parts) > 1: base = parts[-2]
        elif parts: base = Path(parts[0]).stem
        else: base = Path(item_meta.get("bestandspad", "")).stem
        return self._generate_slug(base) or "default-dataset"

    def _prepare_ckan_payload(self, transformed_metadata: Dict[str, Any], field_map: Dict[str, str]) -> Dict[str, Any]:
        """Filters transformed metadata based on field map and handles list formats."""
        payload = {t: transformed_metadata[s] for s, t in field_map.items() if s in transformed_metadata}
        if 'tags' in payload: # Ensure tags format
            tags_val = payload['tags']
            if isinstance(tags_val, list): payload['tags'] = [{'name': str(t).strip()} for t in tags_val if str(t).strip()]
            elif isinstance(tags_val, str): payload['tags'] = [{'name': t.strip()} for t in tags_val.split(',') if t.strip()]
            else: del payload['tags']; log.warning("Invalid tags format removed from payload.")
        if 'groups' in payload and isinstance(payload['groups'], list): # Ensure groups format
            payload['groups'] = [{'id': str(g).strip()} for g in payload['groups'] if str(g).strip()]
        return payload

    def _prepare_upload_info(self, item_meta: Dict[str, Any], transformer: CkanTransformer,
                              ds_map: Dict[str, str], res_map: Dict[str, str]) -> UploadInfo:
        """Transforms metadata, determines targets, prepares payloads for one item."""
        try:
            transformed = transformer.transform(item_meta)
            org_name = transformed.get("owner_org")
            dataset_slug = self._determine_dataset_slug(item_meta)
            ds_payload = self._prepare_ckan_payload(transformed, ds_map)
            res_payload = None; f_path = None; f_hash = None
            if item_meta.get("bestandstype") != 'directory':
                 res_payload = self._prepare_ckan_payload(transformed, res_map)
                 if not res_payload.get('name'): res_payload['name'] = item_meta.get("bestandsnaam", Path(item_meta.get("bestandspad", "")).name)
                 f_path = Path(item_meta.get("bestandspad", ""))
                 f_hash = transformed.get('hash')
            return UploadInfo(True, item_meta, transformed, org_name, dataset_slug, ds_payload, res_payload, f_path, f_hash)
        except Exception as e:
            log.exception(f"Prep error for {item_meta.get('bestandspad')}: {e}")
            return UploadInfo(False, item_meta, {}, None, "", {}, None, None, None, str(e))

    # --- Main Processing Method ---

    def process_list(self, metadata_list: List[Dict[str, Any]], transformer: CkanTransformer,
                     dataset_field_map: Dict[str, str], resource_field_map: Dict[str, str]) -> Tuple[int, int]:
        """Processes list of metadata: prepares, gets/creates orgs/datasets, upserts resources."""
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

            # Ensure Org
            org_id = self.get_or_create_organization(prep_info.target_org_name) if prep_info.target_org_name else None
            if prep_info.target_org_name and not org_id: log.warning(f"{log_prefix}Failed org '{prep_info.target_org_name}'. Proceeding without.")

            # Ensure Dataset
            dataset_id = self.get_or_create_dataset(prep_info.target_dataset_slug, org_id, prep_info.dataset_payload)
            if not dataset_id: log.warning(f"{log_prefix}Failed dataset '{prep_info.target_dataset_slug}'. Skipping resource."); continue
            ds_ids.add(dataset_id)

            # Upsert Resource
            if prep_info.resource_payload and prep_info.file_path:
                resource = self.upsert_resource(dataset_id, prep_info.resource_payload, prep_info.file_path, prep_info.file_hash)
                if resource: res_count += 1
                else: log.warning(f"{log_prefix}Failed resource '{prep_info.resource_payload.get('name')}' in dataset '{prep_info.target_dataset_slug}'.")
            elif item_meta.get("bestandstype") != 'directory': log.warning(f"{log_prefix}File item missing resource info. Skipping resource.")

        log.info(f"CKAN processing summary: {res_count} resources processed, {len(ds_ids)} datasets processed.")
        return res_count, len(ds_ids)

# --- Example Usage --- (Simplified)
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(name)s: %(message)s')
    try:
        import config # Assumes config.py exists and is working
        if not config.config.CKAN_API_URL: raise ValueError("CKAN_API_URL not set")

        log.info("--- Starting CkanUploader Example ---")
        dummy_meta = [
            {'bestandspad': 'test_data/f_a', 'bestandsnaam': 'f_a', 'bestandstype': 'directory', 'relative_path': 'f_a', 'metadata': {}},
            {'bestandspad': 'test_data/f_a/f1.pdf', 'bestandsnaam': 'f1.pdf', 'bestandstype': 'PDF', 'relative_path': 'f_a/f1.pdf', 'metadata': {'titel': 'F1', 'hash': 'h1'}},
            {'bestandspad': 'test_data/rep.xlsx', 'bestandsnaam': 'rep.xlsx', 'bestandstype': 'Excel', 'relative_path': 'rep.xlsx', 'metadata': {'titel': 'Rep', 'hash': 'h2', 'potential_organization_id': 'rep-org'}},
        ]
        for item in dummy_meta:
             p=Path(item['bestandspad']); p.parent.mkdir(parents=True, exist_ok=True)
             if item['bestandstype'] != 'directory' and not p.exists(): p.touch()

        transformer = CkanTransformer(config.config.CKAN_MAPPING)
        uploader = CkanUploader(str(config.config.CKAN_API_URL), config.config.CKAN_API_KEY)

        if uploader.is_connected():
            res_c, ds_c = uploader.process_list(dummy_meta, transformer, config.config.CKAN_DATASET_FIELD_MAP, config.config.CKAN_RESOURCE_FIELD_MAP)
            log.info(f"Example finished. Resources: {res_c}, Datasets: {ds_c}")
        else: log.error("Example failed: No CKAN connection.")
        # shutil.rmtree("test_data", ignore_errors=True)

    except ImportError: log.critical("Failed to import config.py for example.")
    except Exception as e: log.exception(f"Error in example execution: {e}")