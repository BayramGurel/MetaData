# config.py
import os
from dotenv import load_dotenv
from typing import Dict, Optional

load_dotenv()

# --- R-schijf ---
R_SCHIJF_PAD: str = os.getenv("R_SCHIJF_PAD", "")

# --- CKAN ---
CKAN_API_URL: str = os.getenv("CKAN_API_URL", "")
CKAN_API_KEY: str = os.getenv("CKAN_API_KEY", "")

# --- CKAN Metadata Mapping ---
CKAN_MAPPING: Dict[str, str] = {
    "bestandspad": "url",
    "bestandsnaam": "name",
    "bestandstype": "format",
    "relative_path": "resource_path",
    "titel": "title",
    "auteur": "author",
    "onderwerp": "subject",
    "creatiedatum": "created",
    "wijzigingsdatum": "last_modified",
    "tags": "keywords",
    "xmp": "xmp_metadata",
    "werkbladen": "excel_sheets",
    "crs": "crs",
    "bounds": "spatial_bbox",
    "geometrie_types": "geometry_types",
    "file_hash": "hash",
    "aantal_paginas": "num_pages",
    "aantal_werkbladen" : "num_sheets",
    "kolomnamen": "column_names",
    "fout": "error_message",
    "laatst_gewijzigd_door": "last_modified_by",
    "categorie" : "category",
     "producent": "producer",
    "aantal_features": "num_features",
}

# --- Autorisatie Mapping (Metadata-based) ---
AUTHORIZATION_MAPPING: Dict[str, str] = {
    "DepartmentA": "ckan-org-id-for-dept-a",  # Replace with ACTUAL values!
    "DepartmentB": "ckan-org-id-for-dept-b",
    "ProjectX": "ckan-org-id-for-project-x",
}