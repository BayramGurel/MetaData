# load_publish.py
import logging
from typing import Dict, Any, Optional, List  # List added here
from ckanapi import RemoteCKAN, NotFound, NotAuthorized
import config
import extract_transform
import utils
from pathlib import Path

log = logging.getLogger(__name__)


def create_or_update_dataset(ckan: RemoteCKAN, dataset_naam: str, organisatie_id: Optional[str] = None, parent_dataset_id: Optional[str] = None, dataset_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Maakt een dataset aan of werkt deze bij.  Includes parent dataset handling.
    """
    try:
        # Try to update first
        return ckan.action.package_update(id=dataset_naam, owner_org=organisatie_id)
    except NotFound:
        log.info(f"Dataset '{dataset_naam}' not found, creating.")
        dataset_dict = {
            "name": dataset_naam,
            "title": dataset_naam,
            "owner_org": organisatie_id,
            "notes": f"Automatically created dataset for {dataset_naam}",
        }
        if parent_dataset_id:
            dataset_dict["groups"] = [{"id": parent_dataset_id}]

        if dataset_metadata:
            dataset_dict.update(dataset_metadata)
        try:
            return ckan.action.package_create(**dataset_dict)
        except Exception as e:
            log.error(f"Error creating dataset: {e}", exc_info=True)
            raise
    except NotAuthorized:
        log.error("Not authorized for dataset operations.", exc_info=True)
        raise
    except Exception as e:
         log.error(f"Unexpected error during dataset creation/update: {e}", exc_info=True)
         raise



def check_for_resource_changes(ckan: RemoteCKAN, resource_id: str, current_hash: str) -> bool:
    """Checks if a resource has changed based on its hash."""
    try:
        existing_resource = ckan.action.resource_show(id=resource_id)
        existing_hash = existing_resource.get("file_hash")

        if existing_hash and existing_hash == current_hash:
            log.info(f"Resource {resource_id} has not changed (hash match).")
            return False
        else:
             log.info(f"Resource {resource_id} has changed (hash mismatch).")
             return True

    except NotFound:
        log.info(f"Resource {resource_id} not found (new resource).")
        return True
    except Exception as e:
        log.error(f"Error checking resource changes for {resource_id}: {e}", exc_info=True)
        raise

def upload_en_koppel_resource(ckan: RemoteCKAN, dataset_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Uploadt bestand, maakt resource, koppelt metadata (including hash)."""
    bestandspad = Path(metadata["bestandspad"])
    ckan_metadata = extract_transform.transformeer_naar_ckan(metadata, config.CKAN_MAPPING)
    resource_naam = metadata["bestandsnaam"]

    file_hash = metadata["metadata"].get("file_hash")
    if file_hash:
        ckan_metadata["file_hash"] = file_hash

    try:
        existing_resource = None
        try:
            result = ckan.action.resource_search(query=f'name:"{resource_naam}" AND package_id:{dataset_id}')
            if result['count'] > 0:
                existing_resource = result['results'][0]
        except Exception:
             log.info(f"Cannot find the resource")

        if existing_resource:
            resource_changed = check_for_resource_changes(ckan, existing_resource['id'], file_hash)
            if not resource_changed:
                return existing_resource
            else:
                with open(bestandspad, 'rb') as file_to_upload:
                    resource_dict = {
                        'id': existing_resource['id'],
                        'name': resource_naam,
                        'url_type': 'upload',
                        'upload': file_to_upload,
                        **ckan_metadata,
                    }
                return ckan.action.resource_update(**resource_dict)

        else:
            with open(bestandspad, 'rb') as file_to_upload:
                resource_dict = {
                    'package_id': dataset_id,
                    'name': resource_naam,
                    'url_type': 'upload',
                    'upload': file_to_upload,
                    **ckan_metadata,
                }
                return ckan.action.resource_create(**resource_dict)
    except Exception as e:
        log.error(f"Fout bij uploaden/aanmaken resource: {e}", exc_info=True)
        raise



def bepaal_ckan_organisatie(metadata: Dict[str, Any], authorization_mapping: Dict[str, str]) -> Optional[str]:
    """
    Bepaalt de CKAN organisatie ID.
    """
    log = logging.getLogger(__name__)
    organisatie_veld = metadata.get("metadata", {}).get("department")

    if organisatie_veld:
        org_id = authorization_mapping.get(organisatie_veld)
        if org_id:
             return org_id
        else:
            log.warning(f"Geen organisatie-ID gevonden in mapping voor '{organisatie_veld}'. Metadata: {metadata}")
            return None
    else:
        log.warning(f"Metadata veld 'department' niet gevonden in: {metadata}")
        return None


def process_extracted_files(ckan: RemoteCKAN, metadata_lijst: List[Dict[str, Any]], authorization_mapping: Dict[str,str]) -> None:
    """Processes extracted files/directories, creating/updating datasets and resources in CKAN."""

    dataset_cache: Dict[str, str] = {}

    # --- Process Directories (Create Datasets) ---
    for metadata in metadata_lijst:
        if metadata["bestandstype"] == "directory":
            relative_path = metadata["relative_path"]
            parts = Path(relative_path).parts

            current_parent_id = None
            current_path = ""

            for part in parts:
                current_path = str(Path(current_path) / part)
                dataset_naam = part.lower().replace(" ", "-")

                if dataset_naam not in dataset_cache:
                    organisatie_id = bepaal_ckan_organisatie(metadata, authorization_mapping)
                    if not organisatie_id:
                        log.warning(f"Skipping dataset {dataset_naam} (no org).")
                        continue

                    dataset = create_or_update_dataset(ckan, dataset_naam, organisatie_id, current_parent_id, metadata["metadata"])
                    dataset_cache[dataset_naam] = dataset["id"]
                    current_parent_id = dataset["id"]
                else:
                    current_parent_id = dataset_cache[dataset_naam]

    # --- Process Files (Create/Update Resources) ---
    for metadata in metadata_lijst:
        if metadata["bestandstype"] != "directory":
            relative_path = metadata["relative_path"]
            parts = Path(relative_path).parts[:-1]

            if not parts:
                organisatie_id = bepaal_ckan_organisatie(metadata, authorization_mapping)
                if not organisatie_id:
                   log.warning(f"Skipping resource {metadata['bestandsnaam']} (no org).")
                   continue

                dataset_naam = metadata['bestandsnaam'].rsplit('.', 1)[0].lower().replace(" ", "-")
                if dataset_naam not in dataset_cache:
                    dataset = create_or_update_dataset(ckan, dataset_naam, organisatie_id)
                    dataset_cache[dataset_naam] = dataset["id"]
                else:
                    dataset = {"id": dataset_cache[dataset_naam]}
                resource = upload_en_koppel_resource(ckan, dataset["id"], metadata)
                log.info(f"Resource processed: {metadata['bestandsnaam']} -> Dataset: {dataset_naam}")

            else:
                dataset_naam = parts[-1].lower().replace(" ", "-")
                if dataset_naam in dataset_cache:
                    dataset_id = dataset_cache[dataset_naam]
                    resource = upload_en_koppel_resource(ckan, dataset_id, metadata)
                    log.info(f"Resource processed: {metadata['bestandsnaam']} -> Dataset: {dataset_naam}")
                else:
                    log.error(f"Dataset {dataset_naam} not found for resource {metadata['bestandsnaam']}.")