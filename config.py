# config.py
import os
from dotenv import load_dotenv
from typing import Dict

load_dotenv()

R_SCHIJF_PAD: str = os.getenv("R_SCHIJF_PAD", "")
CKAN_API_URL: str = os.getenv("CKAN_API_URL", "")
CKAN_API_KEY: str = os.getenv("CKAN_API_KEY", "")

CKAN_MAPPING: Dict[str, str] = {
    "bestandspad": "url",
    "bestandsnaam": "name",
    "bestandstype": "format",
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
}

# Metadata-based authorization mapping.  Adapt these keys and values!
AUTHORIZATION_MAPPING: Dict[str, str] = {
    "DepartmentA": "ckan-org-id-for-dept-a",  # Replace with actual values
    "DepartmentB": "ckan-org-id-for-dept-b",
    "ProjectX": "ckan-org-id-for-project-x",
    # Add more mappings as needed
}