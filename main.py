import logging
from typing import List, Dict, Any
import config
import utils
import extract_transform
import load_publish
from ckanapi import RemoteCKAN

log = utils.setup_logging()  # Initialiseer logging


def run_pipeline() -> None:
    """Voert de volledige data pipeline uit."""
    try:
        ckan = RemoteCKAN(config.CKAN_API_URL, apikey=config.CKAN_API_KEY)

        log.info("Start data pipeline...")
        metadata_lijst: List[Dict[str, Any]] = extract_transform.scan_en_extraheer(config.R_SCHIJF_PAD)

        for metadata in metadata_lijst:
            validatie_resultaten = extract_transform.valideer_metadata(metadata)
            if validatie_resultaten:
                log.warning(f"Validatiefouten voor {metadata['bestandspad']}: {validatie_resultaten}")
                continue  # Sla ongeldige bestanden over

            organisatie_id = load_publish.bepaal_ckan_organisatie(metadata, config.AUTHORIZATION_MAPPING)
            if not organisatie_id:
                log.warning(f"Geen organisatie-ID gevonden voor {metadata['bestandspad']}, overslaan.")
                continue

            #Gebruik bestandsnaam (zonder extensie) als unieke dataset naam
            dataset_naam = metadata['bestandsnaam'].rsplit('.', 1)[0]

            dataset = load_publish.create_or_update_dataset(ckan, dataset_naam, organisatie_id)
            resource = load_publish.upload_en_koppel_resource(ckan, dataset["id"], metadata)

            log.info(f"Bestand verwerkt: {metadata['bestandspad']} -> Dataset: {dataset['id']}, Resource: {resource['id']}")

        log.info("Data pipeline voltooid.")

    except Exception as e:
        log.exception("Fout tijdens uitvoeren pipeline: %s", e) #Gebruik exception om de volledige stacktrace te loggen

if __name__ == "__main__":
    run_pipeline()