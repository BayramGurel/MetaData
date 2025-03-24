# config.py
import os
from dotenv import load_dotenv
from typing import Dict, Optional, Any
import logging  # Import logging

load_dotenv()

log = logging.getLogger(__name__)

# --- R-schijf ---
R_SCHIJF_PAD: str = os.getenv("R_SCHIJF_PAD", "")
if not R_SCHIJF_PAD:
    log.error("R_SCHIJF_PAD is not set. Please set it in your .env file or environment.")
    # Consider exiting here, as the pipeline cannot run without this.
    # raise ValueError("R_SCHIJF_PAD environment variable is not set.") #Optionally raise error

# --- CKAN ---
CKAN_API_URL: str = os.getenv("CKAN_API_URL", "")
if not CKAN_API_URL:
    log.error("CKAN_API_URL is not set. Please set it in your .env file or environment.")
    # raise ValueError("CKAN_API_URL environment variable is not set.") #Optionally raise error
CKAN_API_KEY: Optional[str] = os.getenv("CKAN_API_KEY")  # API Key is optional


# --- CKAN Metadata Mapping ---
#  Using Any for the second type argument to allow different value types
CKAN_MAPPING: Dict[str, str] = {
    "bestandspad": "url",
    "bestandsnaam": "name",
    "bestandstype": "format",
    "relative_path": "resource_path",  # Better name
    "titel": "title",
    "auteur": "author",
    "onderwerp": "subject",
    "creatiedatum": "created",
    "wijzigingsdatum": "last_modified",
    "tags": "keywords",
    "xmp": "xmp_metadata",
    "werkbladen": "excel_sheets",  # More descriptive
    "crs": "crs",
    "bounds": "spatial_bbox",  # Or "spatial" if using GeoJSON
    "geometrie_types": "geometry_types",
    "file_hash": "hash",  # More standard name
    "aantal_paginas": "num_pages",
    "aantal_werkbladen": "num_sheets",
    "kolomnamen": "column_names",
    "fout": "error_message",  # For error reporting
    "laatst_gewijzigd_door": "last_modified_by",
    "categorie": "category",
    "producent": "producer",
    "aantal_features": "num_features",
}