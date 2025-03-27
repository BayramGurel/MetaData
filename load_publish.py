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
    from extract_transform import CkanTransformer # For type hinting
except ImportError:
    logging.warning("CkanTransformer not imported. Type hinting incomplete.")
    class CkanTransformer: # Dummy for type hint
        def __init__(self, *args, **kwargs): pass
        def transform(self, metadata: Dict[str, Any]) -> Dict[str, Any]: return metadata
# --- End Dependency Imports ---

log = logging.getLogger(__name__)

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
            self.ckan.action.site_read() # Test connection
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
                return self.ckan.action.__getattribute__(action)(**params)
            except NotFound: return None
            except (NotAuthorized, ValidationError) as e: raise e # Fail immediately
            except CKANAPIError as e:
                log.warning(f"{log_prefix}CKAN API Error ({action}): {e}")
                last_exception = e
            except Exception as e:
                log.warning(f"{log_prefix}Unexpected Error ({action}): {e}", exc_info=log.level <= logging.DEBUG)
                last_exception = e

            if attempt < retries: time.sleep(delay)
            else: # Last attempt failed
                 log.error(f"{log_prefix}CKAN action '{action}' failed after {retries+1} attempts.")
                 if last_exception: raise last_exception # type: ignore
                 else: raise CKANAPIError(f"Action '{action}' failed.") # Should not happen

    # --- Org/Dataset/Resource Get/Create/Update Methods (largely unchanged logic, focus on clarity) ---

    def get_or_create_organization(self, org_name: str, org_title: Optional[str] = None, description: Optional[str] = None) -> Optional[str]:
        """Gets or creates an organization, caches result. Returns ID or None."""
        log_prefix = f"[Org: {org_name}] "
        if not org_name: return None
        if org_name in self.org_cache: return self.org_cache[org_name]

        try:
            org_data = self._execute_action("organization_show", {"id": org_name}, log_prefix)
            if org_data and (org_id := org_data.get("id")):
                 log.info(f"{log_prefix}Found (ID: {org_id})"); self.org_cache[org_name] = org_id; return org_id
            # Not found, try creating
            if not self.api_key: log.error(f"{log_prefix}Cannot create: API key missing."); self.org_cache[org_name] = None; return None
            log.info(f"{log_prefix}Not found, creating...")
            payload = {"name": org_name, "title": org_title or org_name.replace('-', ' ').title(),
                       "description": description or f"Org: {org_title or org_name}", "state": "active"}
            new_org = self._execute_action("organization_create", payload, log_prefix)
            if new_org and (new_id := new_org.get("id")):
                 log.info(f"{log_prefix}Created (ID: {new_id})"); self.org_cache[org_name] = new_id; return new_id
            else: log.error(f"{log_prefix}Create failed or returned no ID."); self.org_cache[org_name] = None; return None
        except (NotAuthorized, ValidationError, CKANAPIError, ConnectionError, Exception) as e:
             log.error(f"{log_prefix}Error ensuring org exists: {e}")
             self.org_cache[org_name] = None; return None

    def get_or_create_dataset(self, dataset_slug: str, org_id: Optional[str], dataset_payload: Dict[str, Any]) -> Optional[str]:
        """Gets/Creates/Updates a dataset, caches result. Returns ID or None."""
        log_prefix = f"[Dataset: {dataset_slug}] "
        if not dataset_slug: return None
        if dataset_slug in self.dataset_cache: return self.dataset_cache[dataset_slug]

        try:
            existing_ds = self._execute_action("package_show", {"id": dataset_slug}, log_prefix)
            payload = dataset_payload.copy(); payload['name'] = dataset_slug # Ensure correct slug

            if existing_ds: # Update
                 existing_id = existing_ds.get("id")
                 if not existing_id: log.error(f"{log_prefix}Show OK but no ID?"); self.dataset_cache[dataset_slug] = None; return None
                 log.info(f"{log_prefix}Found (ID: {existing_id}), updating...")
                 if org_id and existing_ds.get('owner_org') != org_id:
                      log.warning(f"{log_prefix}Org mismatch! Expected {org_id}, got {existing_ds.get('owner_org')}. Skipping update.")
                      self.dataset_cache[dataset_slug] = None; return None # Skip update due to org mismatch
                 payload['id'] = existing_id; payload['owner_org'] = org_id # Ensure update uses ID and correct org
                 updated_ds = self._execute_action("package_update", payload, log_prefix)
                 if updated_ds and (upd_id := updated_ds.get("id")): log.info(f"{log_prefix}Updated (ID: {upd_id})"); self.dataset_cache[dataset_slug] = upd_id; return upd_id
                 else: log.error(f"{log_prefix}Update failed or returned no ID."); self.dataset_cache[dataset_slug] = None; return None
            else: # Create
                 log.info(f"{log_prefix}Not found, creating...")
                 if not self.api_key: log.error(f"{log_prefix}Cannot create: API key missing."); self.dataset_cache[dataset_slug] = None; return None
                 if org_id: payload['owner_org'] = org_id
                 elif 'owner_org' in payload: del payload['owner_org'] # Don't send owner_org=None
                 if not payload.get('title'): payload['title'] = dataset_slug.replace('-', ' ').title() # Default title
                 new_ds = self._execute_action("package_create", payload, log_prefix)
                 if new_ds and (new_id := new_ds.get("id")): log.info(f"{log_prefix}Created (ID: {new_id})"); self.dataset_cache[dataset_slug] = new_id; return new_id
                 else: log.error(f"{log_prefix}Create failed or returned no ID."); self.dataset_cache[dataset_slug] = None; return None
        except (NotAuthorized, ValidationError, CKANAPIError, ConnectionError, Exception) as e:
             # Handle specific case of create failing due to race condition
             if not existing_ds and isinstance(e, ValidationError) and 'name' in getattr(e, 'error_dict', {}):
                  if any('already exists' in msg.lower() for msg in e.error_dict['name']):
                       log.warning(f"{log_prefix}Create conflict (likely race condition), trying fetch again.")
                       try: # Try fetch again immediately
                            refetched = self._execute_action("package_show", {"id": dataset_slug}, log_prefix)
                            if refetched and (ref_id := refetched.get("id")):
                                 self.dataset_cache[dataset_slug] = ref_id; return ref_id
                       except Exception as fetch_e: log.error(f"{log_prefix}Refetch after conflict failed: {fetch_e}")
             log.error(f"{log_prefix}Error ensuring dataset exists: {e}") # Log other errors
             self.dataset_cache[dataset_slug] = None; return None

    def _resource_needs_update(self, res_id: str, current_hash: Optional[str], payload: Dict[str, Any], log_prefix: str) -> bool:
        """Checks if resource needs update (hash or metadata)."""
        try:
            existing = self._execute_action("resource_show", {"id": res_id}, log_prefix)
            if not existing: return True # Treat not found as needing update/create

            # 1. Check Hash
            if current_hash and existing.get("hash") == current_hash:
                 log.info(f"{log_prefix}Hash match. Checking metadata.") # Continue check
            elif current_hash:
                 log.info(f"{log_prefix}Hash mismatch/missing. Update needed.")
                 return True # Hash differs or was missing

            # 2. Check Metadata fields present in payload (simple comparison)
            for k, v in payload.items():
                 if k in ['id', 'package_id', 'upload'] or k.startswith('_'): continue
                 if existing.get(k) != v:
                      log.info(f"{log_prefix}Metadata diff on '{k}'. Update needed.")
                      return True # Metadata differs

            log.info(f"{log_prefix}Hash & metadata match. No update needed.")
            return False # No changes detected

        except Exception as e:
            log.error(f"{log_prefix}Error checking resource status: {e}. Assuming update needed.")
            return True # Assume update needed if check fails


    def upsert_resource(self, dataset_id: str, resource_payload: Dict[str, Any],
                        file_path: Path, file_hash: Optional[str]) -> Optional[Dict[str, Any]]:
        """Creates/Updates resource, uploads file. Returns resource dict or None."""
        res_name = resource_payload.get("name")
        log_prefix = f"[DS:{dataset_id}][Res:{res_name}] "
        if not res_name: log.error(f"{log_prefix}Payload missing 'name'."); return None
        if not file_path.is_file(): log.error(f"{log_prefix}File not found: {file_path}."); return None

        abs_path_str = str(file_path.resolve())
        if abs_path_str in self.processed_resource_paths:
             log.info(f"{log_prefix}Already processed path in this run: {abs_path_str}. Skipping.")
             return None # Avoid re-processing same file path

        try:
            # --- Find Existing ---
            existing_res = None
            search = self._execute_action("resource_search", {"query": f'name:"{res_name}" AND package_id:"{dataset_id}"'}, log_prefix)
            if search and search.get('count', 0) > 0: existing_res = search['results'][0]

            payload = resource_payload.copy(); payload['package_id'] = dataset_id # Ensure dataset_id
            action = "resource_create"; payload.pop('id', None) # Default to create

            # --- Decide Action ---
            if existing_res and (res_id := existing_res.get("id")):
                 payload['id'] = res_id; action = "resource_update"
                 if not self._resource_needs_update(res_id, file_hash, payload, log_prefix):
                      self.processed_resource_paths.add(abs_path_str); return existing_res # No change needed
            elif existing_res: # Found but no ID? Problematic.
                 log.error(f"{log_prefix}Found existing resource but it lacks an ID. Cannot update."); return None

            # --- Execute with Upload ---
            log_info_verb = "Updating" if action == "resource_update" else "Creating"
            log.info(f"{log_prefix}{log_info_verb} resource...")
            with file_path.open('rb') as file_obj:
                payload['upload'] = file_obj # Attach file for upload
                if file_hash and 'hash' in payload: payload['hash'] = file_hash # Ensure hash is in payload
                upserted = self._execute_action(action, payload, log_prefix)

            if upserted and upserted.get("id"):
                log.info(f"{log_prefix}Successfully {log_info_verb.lower()}d (ID: {upserted['id']}).")
                self.processed_resource_paths.add(abs_path_str); return upserted
            else: log.error(f"{log_prefix}{log_info_verb} failed or returned invalid data."); return None

        except (FileNotFoundError, PermissionError) as e: log.error(f"{log_prefix}File error: {e}"); return None
        except (NotAuthorized, ValidationError, CKANAPIError, ConnectionError, Exception) as e:
             log.error(f"{log_prefix}Failed to upsert resource: {e}") # Details should be logged by _execute_action if raised
             return None


    # --- Main Processing Logic ---

    def _generate_slug(self, name: str, max_length: int = 100) -> str:
        """Generates CKAN-friendly slug."""
        if not name: return "unnamed"
        slug = str(name).lower().strip(); slug = re.sub(r'[\s\._]+', '-', slug); slug = re.sub(r'[^\w-]', '', slug); slug = re.sub(r'-{2,}', '-', slug); slug = slug.strip('-')[:max_length]; return slug or "unnamed"

    def _determine_dataset_slug(self, item_meta: Dict[str, Any]) -> str:
        """Determines target dataset slug based on item metadata (path/type)."""
        # Strategy: Use parent dir name for files, dir name for dirs, fallback to filename stem
        rel_path = item_meta.get("relative_path", ""); item_type = item_meta.get("bestandstype", "")
        base = ""; parts = Path(rel_path).parts
        if item_type == 'directory': base = parts[-1] if parts else ""
        elif len(parts) > 1: base = parts[-2] # File in subdir -> use parent dir name
        elif parts: base = Path(parts[0]).stem # File in root -> use file stem
        else: base = Path(item_meta.get("bestandspad", "")).stem # Fallback if no rel path
        return self._generate_slug(base) or "default-dataset"

    def _prepare_upload_info(self, item_meta: Dict[str, Any], transformer: CkanTransformer,
                              ds_map: Dict[str, str], res_map: Dict[str, str]) -> UploadInfo:
        """Transforms metadata, determines targets, prepares payloads for one item."""
        try:
            transformed = transformer.transform(item_meta)
            org_name = transformed.get("owner_org") # Assumes transformer maps this
            dataset_slug = self._determine_dataset_slug(item_meta)

            # Prepare filtered payloads
            dataset_payload = {t: transformed[s] for s, t in ds_map.items() if s in transformed}
            resource_payload = None; file_path = None; file_hash = None

            if item_meta.get("bestandstype") != 'directory':
                 resource_payload = {t: transformed[s] for s, t in res_map.items() if s in transformed}
                 # Ensure resource name exists
                 if not resource_payload.get('name'):
                      resource_payload['name'] = item_meta.get("bestandsnaam", Path(item_meta.get("bestandspad", "")).name)
                 file_path = Path(item_meta.get("bestandspad", ""))
                 file_hash = transformed.get('hash')

            return UploadInfo(success=True, item_meta=item_meta, transformed_meta=transformed,
                              target_org_name=org_name, target_dataset_slug=dataset_slug,
                              dataset_payload=dataset_payload, resource_payload=resource_payload,
                              file_path=file_path, file_hash=file_hash)

        except Exception as e:
            log.exception(f"Failed to prepare upload info for {item_meta.get('bestandspad')}: {e}")
            return UploadInfo(success=False, item_meta=item_meta, transformed_meta={},
                              target_org_name=None, target_dataset_slug="",
                              dataset_payload={}, resource_payload=None, file_path=None, file_hash=None,
                              error_message=str(e))


    def process_list(self, metadata_list: List[Dict[str, Any]], transformer: CkanTransformer,
                     dataset_field_map: Dict[str, str], resource_field_map: Dict[str, str]) -> Tuple[int, int]:
        """Processes list of metadata: prepares, gets/creates orgs/datasets, upserts resources."""
        if not self.is_connected(): log.error("CKAN not connected."); return 0, 0

        res_count, ds_ids = 0, set()
        log.info(f"Processing {len(metadata_list)} items for CKAN upload...")

        for item_meta in metadata_list:
            prep_info = self._prepare_upload_info(item_meta, transformer, dataset_field_map, resource_field_map)

            if not prep_info.success:
                 log.error(f"Skipping item due to preparation error: {item_meta.get('bestandspad')} ({prep_info.error_message})")
                 continue

            log_prefix = f"[Item: {item_meta.get('relative_path', '?')}] "
            log.debug(f"{log_prefix}Prepared: Org='{prep_info.target_org_name}', DS='{prep_info.target_dataset_slug}'")

            # --- Ensure Org ---
            org_id: Optional[str] = None
            if prep_info.target_org_name:
                 # Simple title/desc for org creation if needed
                 title = prep_info.transformed_meta.get("owner_org_title", prep_info.target_org_name.replace('-', ' ').title())
                 desc = f"Org for data source '{prep_info.target_org_name}'"
                 org_id = self.get_or_create_organization(prep_info.target_org_name, title, desc)
                 if not org_id: log.warning(f"{log_prefix}Failed get/create org '{prep_info.target_org_name}'. Proceeding without (may fail).")
            # else: No org specified, proceed without org_id

            # --- Ensure Dataset ---
            dataset_id = self.get_or_create_dataset(prep_info.target_dataset_slug, org_id, prep_info.dataset_payload)
            if not dataset_id:
                 log.warning(f"{log_prefix}Failed get/create dataset '{prep_info.target_dataset_slug}'. Skipping resource.")
                 continue
            ds_ids.add(dataset_id) # Add successfully processed dataset ID

            # --- Upsert Resource (if applicable) ---
            if prep_info.resource_payload and prep_info.file_path:
                resource_data = self.upsert_resource(
                    dataset_id=dataset_id,
                    resource_payload=prep_info.resource_payload,
                    file_path=prep_info.file_path,
                    file_hash=prep_info.file_hash
                )
                if resource_data: res_count += 1
                else: log.warning(f"{log_prefix}Failed upsert resource '{prep_info.resource_payload.get('name')}' to dataset '{prep_info.target_dataset_slug}'.")
            elif item_meta.get("bestandstype") != 'directory':
                 log.warning(f"{log_prefix}Item is a file but resource payload/path missing after preparation. Skipping resource.")

        log.info(f"CKAN processing summary: {res_count} resources processed, {len(ds_ids)} datasets processed.")
        return res_count, len(ds_ids)


# --- Example Usage --- (Simplified)
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(name)s: %(message)s')
    try:
        # Assumes config.py exists and provides necessary constants/maps
        import config
        if not config.CKAN_API_URL: raise ValueError("CKAN_API_URL not set in config")

        log.info("--- Starting CkanUploader Example ---")
        # Dummy data (replace with actual data from scanner)
        dummy_meta = [
            {'bestandspad': 'test_data/folder_a', 'bestandsnaam': 'folder_a', 'bestandstype': 'directory', 'relative_path': 'folder_a', 'metadata': {}},
            {'bestandspad': 'test_data/folder_a/file1.pdf', 'bestandsnaam': 'file1.pdf', 'bestandstype': 'PDF', 'relative_path': 'folder_a/file1.pdf', 'metadata': {'titel': 'File One PDF', 'file_hash': 'hash1'}},
            {'bestandspad': 'test_data/report.xlsx', 'bestandsnaam': 'report.xlsx', 'bestandstype': 'Excel', 'relative_path': 'report.xlsx', 'metadata': {'titel': 'Main Report', 'file_hash': 'hash2', 'potential_organization_id': 'reports-org'}},
        ]
        # Create dummy files if needed for testing
        for item in dummy_meta:
             p = Path(item['bestandspad']); p.parent.mkdir(parents=True, exist_ok=True)
             if item['bestandstype'] != 'directory' and not p.exists(): p.touch()

        transformer = CkanTransformer(config.CKAN_MAPPING)
        uploader = CkanUploader(config.CKAN_API_URL, config.CKAN_API_KEY)

        if uploader.is_connected():
            res_c, ds_c = uploader.process_list(
                dummy_meta, transformer, config.CKAN_DATASET_FIELD_MAP, config.CKAN_RESOURCE_FIELD_MAP
            )
            log.info(f"Example finished. Resources: {res_c}, Datasets: {ds_c}")
        else: log.error("Example failed: No CKAN connection.")

        # Simple cleanup (optional)
        # shutil.rmtree("test_data", ignore_errors=True)

    except ImportError: log.critical("Failed to import config.py for example.")
    except Exception as e: log.exception(f"Error in example execution: {e}")