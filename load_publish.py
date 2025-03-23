import logging
from typing import Dict, Any, Optional
from ckanapi import RemoteCKAN, NotFound, NotAuthorized
import config
import extract_transform
from pathlib import Path

log = logging.getLogger(__name__)


def create_or_update_dataset(ckan: RemoteCKAN, dataset_naam: str, organisatie_id: Optional[str] = None) -> Dict[str, Any]:
    """Maakt een dataset aan of werkt deze bij als hij al bestaat."""
    try:
        # Probeer eerst bij te werken (sneller als de dataset bestaat)
        return ckan.action.package_update(id=dataset_naam, owner_org=organisatie_id)
    except NotFound:
        # Zo niet gevonden, maak dan aan
        log.info(f"Dataset '{dataset_naam}' niet gevonden, wordt aangemaakt.")
        dataset_dict = {
            "name": dataset_naam,
            "title": dataset_naam,
            "owner_org": organisatie_id,
        }
        try:
            return ckan.action.package_create(**dataset_dict)
        except Exception as e:
            log.error(f"Fout bij aanmaken dataset: {e}", exc_info=True)
            raise
    except NotAuthorized:
        log.error("Niet geautoriseerd voor dataset bewerkingen.", exc_info=True)
        raise
    except Exception as e:
         log.error(f"Onverwachte fout bij dataset creatie/update: {e}", exc_info=True)
         raise



def upload_en_koppel_resource(ckan: RemoteCKAN, dataset_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Uploadt bestand, maakt resource, koppelt metadata."""
    bestandspad = Path(metadata["bestandspad"])  # Gebruik Path object
    ckan_metadata = extract_transform.transformeer_naar_ckan(metadata, config.CKAN_MAPPING)
    resource_naam = metadata["bestandsnaam"]

    try:
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
    Bepaalt de CKAN organisatie ID op basis van metadata en de autorisatie mapping.
    """
    log = logging.getLogger(__name__)

    # --- AANPASSEN: Kies het juiste metadata veld ---
    #  Vervang 'department' door de naam van het veld in *jouw* metadata
    #  dat de organisatie-informatie bevat.  Bijvoorbeeld: 'project', 'owner', 'source', etc.
    organisatie_veld = metadata.get("metadata", {}).get("department")

    if organisatie_veld:
        org_id = authorization_mapping.get(organisatie_veld)
        if org_id:
             return org_id
        else:
            log.warning(f"Geen organisatie-ID gevonden in mapping voor '{organisatie_veld}'. Metadata: {metadata}")
            return None
    else:
        log.warning(f"Metadata veld 'department' niet gevonden in: {metadata}")  # Aangepast veld
        return None