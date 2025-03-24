# load_publish.py
import logging
from typing import Dict, Any, Optional, List
from ckanapi import RemoteCKAN, NotFound, NotAuthorized, CKANAPIError
import config
import extract_transform  #  For type hinting if needed
import utils # For type hinting
from pathlib import Path

log = logging.getLogger(__name__)


def _ckan_action(ckan: RemoteCKAN, action: str, data_dict: Dict[str, Any] = None, context: Dict[str, Any] = None,
                  log_prefix: str = "") -> Any:
    """Helper for CKAN API calls (same as before - no changes here)."""
    try:
        return ckan.action.__getattribute__(action)(data_dict=data_dict, context=context)
    except NotFound:
        return None
    except (NotAuthorized, CKANAPIError) as e:
        log.error(f"{log_prefix}CKAN API Error ({action}): {e}", exc_info=True)
        raise
    except Exception as e:
        log.error(f"{log_prefix}Unexpected error in CKAN action ({action}): {e}", exc_info=True)
        raise


def get_or_create_organization(ckan: RemoteCKAN, org_name: str, org_title: Optional[str] = None) -> Optional[str]:
    """Gets or creates a CKAN organization. Returns the organization ID."""
    log_prefix = f"[Org: {org_name}] "  # For consistent logging

    org = _ckan_action(ckan, "organization_show", {"id": org_name}, log_prefix=log_prefix)
    if org:
        log.info(f"{log_prefix}Exists (ID: {org['id']})")
        return org["id"]

    log.info(f"{log_prefix}Not found, creating.")
    org_dict = {
        "name": org_name,
        "title": org_title or org_name,
        "description": f"Automatically created organization for ZIP file '{org_name}'",  # More descriptive
    }
    new_org = _ckan_action(ckan, "organization_create", org_dict, log_prefix=log_prefix)
    if new_org:
        log.info(f"{log_prefix}Created (ID: {new_org['id']})")
        return new_org["id"]
    return None #Explicit None on failure.


def get_or_create_dataset(ckan: RemoteCKAN, dataset_name: str, org_id: Optional[str] = None,
                          parent_dataset_id: Optional[str] = None,
                          dataset_metadata: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Gets or creates a dataset.  Handles parent-child relationships."""
    log_prefix = f"[Dataset: {dataset_name}] "

    # Check if dataset exists (considering organization)
    context = {'ignore_auth': True} if org_id else None
    dataset = _ckan_action(ckan, "package_show", {"id": dataset_name}, context=context, log_prefix=log_prefix)

    if dataset:
        if org_id and dataset.get('owner_org') != org_id:
            log.info(f"{log_prefix}Exists, but wrong organization. Creating new.")
            dataset = None  # Force creation of a new dataset
        else:  # Dataset Exists and org is correct.
            log.info(f"{log_prefix}Exists, updating.")
            update_params = {"id": dataset['id']}
            if org_id:
                update_params["owner_org"] = org_id
            if dataset_metadata:
                update_params.update(dataset_metadata)
            if parent_dataset_id:
                update_params["groups"] = [{"id": parent_dataset_id}]
            return _ckan_action(ckan, "package_update", update_params, log_prefix=log_prefix)

    # Dataset does not exist, create it
    log.info(f"{log_prefix}Not found, creating.")
    dataset_dict = {
        "name": dataset_name,
        "title": dataset_name,
        "owner_org": org_id,
        "notes": f"Automatically created dataset for {dataset_name}",
        "groups": [{"id": parent_dataset_id}] if parent_dataset_id else [],
    }
    if dataset_metadata:
        dataset_dict.update(dataset_metadata)
    return _ckan_action(ckan, "package_create", dataset_dict, log_prefix=log_prefix)

def _resource_has_changed(ckan: RemoteCKAN, resource_id: str, current_hash: str) -> bool:
    """Checks if a resource's hash has changed.  Internal helper function."""
    log_prefix = f"[Resource: {resource_id}] "
    existing_resource = _ckan_action(ckan, "resource_show", {"id": resource_id}, log_prefix=log_prefix)

    if existing_resource:
        existing_hash = existing_resource.get("file_hash")
        if existing_hash and existing_hash == current_hash:
            log.info(f"{log_prefix}Has not changed (hash match).")
            return False
        log.info(f"{log_prefix}Has changed (hash mismatch).")
        return True
    log.info(f"{log_prefix}Not found (new resource).")
    return True  # Treat NotFound as changed (new resource)

def upsert_resource(ckan: RemoteCKAN, dataset_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Creates or updates (upserts) a resource, handling file uploads and metadata."""
    bestandspad = Path(metadata["bestandspad"])
    ckan_metadata = extract_transform.transformeer_naar_ckan(metadata, config.CKAN_MAPPING)
    resource_naam = metadata["bestandsnaam"]
    file_hash = metadata["metadata"].get("file_hash")
    if file_hash:
        ckan_metadata["file_hash"] = file_hash

    log_prefix = f"[Resource: {resource_naam}] "

    # Find Existing Resource.
    result = _ckan_action(ckan, "resource_search", {"query": f'name:"{resource_naam}" AND package_id:{dataset_id}'}, log_prefix=log_prefix)
    existing_resource = result.get('results', [None])[0] if result else None

    with open(bestandspad, 'rb') as file_to_upload:  # Open file *once*
        if existing_resource:
            if not _resource_has_changed(ckan, existing_resource["id"], file_hash):
                return existing_resource # No Change

            log.info(f"{log_prefix}Updating existing resource.")
            resource_dict = {
                'id': existing_resource['id'],
                'name': resource_naam,
                'url_type': 'upload',
                'upload': file_to_upload,
                **ckan_metadata
            }
            return _ckan_action(ckan, "resource_update", resource_dict, log_prefix=log_prefix)
        else:
            log.info(f"{log_prefix}Creating new resource.")
            resource_dict = {
                'package_id': dataset_id,
                'name': resource_naam,
                'url_type': 'upload',
                'upload': file_to_upload,
                **ckan_metadata
            }
            return _ckan_action(ckan, "resource_create", resource_dict, log_prefix=log_prefix)



def process_extracted_files(ckan: RemoteCKAN, metadata_list: List[Dict[str, Any]]) -> None:
    """Processes files/dirs, creating/updating datasets and resources in CKAN."""

    dataset_cache: Dict[str, str] = {}
    org_cache: Dict[str, str] = {}  # Cache org IDs

    for metadata in metadata_list:
        # --- Determine Organization (from ZIP file name) ---
        potential_org_id = metadata.get("metadata", {}).get("potential_organization_id")
        org_id = None
        if potential_org_id:
            if potential_org_id not in org_cache:
                org_id = get_or_create_organization(ckan, potential_org_id)
                if org_id:
                    org_cache[potential_org_id] = org_id
            else:
                org_id = org_cache[potential_org_id]

        if metadata["bestandstype"] == "directory":
            # --- Handle Directories (Hierarchical Datasets) ---
            current_parent_id = None
            current_path = ""
            for part in Path(metadata["relative_path"]).parts:
                current_path = str(Path(current_path) / part)
                dataset_name = part.lower().replace(" ", "-")

                if dataset_name in dataset_cache:
                    current_parent_id = dataset_cache[dataset_name]
                    continue
                # Use the ZIP-derived org_id if available, otherwise None
                dataset = get_or_create_dataset(ckan, dataset_name, org_id, current_parent_id, metadata["metadata"])
                if dataset:
                    dataset_cache[dataset_name] = dataset["id"]
                    current_parent_id = dataset["id"]

        elif metadata["bestandstype"] != "directory":
            # --- Handle Files (Resources) ---
            parts = Path(metadata["relative_path"]).parts
            if not parts:  # File at root of ZIP
                dataset_name = metadata['bestandsnaam'].rsplit('.', 1)[0].lower().replace(" ", "-")
            else:
                dataset_name = parts[-1].lower().replace(" ", "-")  # Dataset name based on last part of path

            if dataset_name not in dataset_cache:
                # Use ZIP-derived org_id.  If org_id is None, dataset creation will likely fail,
                # which is acceptable behavior (no org to associate with).
                dataset = get_or_create_dataset(ckan, dataset_name, org_id)
                if dataset:
                    dataset_cache[dataset_name] = dataset["id"]
            dataset_id = dataset_cache.get(dataset_name)
            if not dataset_id:
                log.warning(f"No dataset for resource: {metadata['bestandsnaam']}. Skipping.")
                continue

            upsert_resource(ckan, dataset_id, metadata)
            log.info(f"Processed: {metadata['bestandsnaam']} -> Dataset: {dataset_name}")