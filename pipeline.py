import configparser
import logging
import os
import shutil
import sys
import time
import zipfile
from pathlib import Path

# Probeer ckanapi te importeren
try:
    from ckanapi import RemoteCKAN, NotAuthorized, NotFound, ValidationError
except ImportError:
    print("Fout: 'ckanapi' module niet gevonden.")
    print("Installeer deze met: pip install ckanapi")
    sys.exit(1)

# --- Configuratie Laden ---
config = configparser.ConfigParser()
script_dir = Path(__file__).parent
config_path = script_dir / 'config.ini'

if not config_path.is_file():
    print(f"Fout: Configuratiebestand 'config.ini' niet gevonden in {script_dir}")
    sys.exit(1)

try:
    config.read(config_path)

    # Paden
    SOURCE_DIR = Path(config.get('Paths', 'source_dir'))
    STAGING_DIR = Path(config.get('Paths', 'staging_dir'))
    PROCESSED_DIR_STR = config.get('Paths', 'processed_dir', fallback=None)  # Lees als string
    LOG_FILE_REL = config.get('Paths', 'log_file')  # Relatief pad uit config

    # Maak log path absoluut t.o.v. script dir
    LOG_FILE = (script_dir / LOG_FILE_REL).resolve()

    # CKAN
    CKAN_URL = config.get('CKAN', 'ckan_url')
    CKAN_API_KEY = config.get('CKAN', 'api_key')
    CKAN_OWNER_ORG = config.get('CKAN', 'owner_org')

    # --- !!! SECURITY WARNING !!! ---
    if CKAN_API_KEY == 'JOUW_CKAN_API_KEY_HIER' or not CKAN_API_KEY:
        print("WAARSCHUWING: CKAN API Key is niet ingesteld in config.ini of is de placeholder.")
        # Optioneel: sys.exit(1) als je wilt stoppen zonder API key
    elif len(CKAN_API_KEY) < 20:  # Zeer simpele check op placeholder vs echte key
        print("WAARSCHUWING: De ingestelde CKAN API key lijkt erg kort. Controleer config.ini.")
    print("-" * 30)
    print("WAARSCHUWING: Hardcodeer API keys NOOIT in scripts voor productie of versiebeheer.")
    print("Gebruik environment variables of een secrets manager.")
    print("-" * 30)
    # --- !!! END SECURITY WARNING !!! ---

    # Pipeline
    MOVE_PROCESSED = config.getboolean('Pipeline', 'move_processed_files', fallback=False)
    PROCESSED_DIR = Path(PROCESSED_DIR_STR) if MOVE_PROCESSED and PROCESSED_DIR_STR else None

    # Zip Handling
    RELEVANT_EXTENSIONS_STR = config.get('ZipHandling', 'relevant_extensions', fallback='')
    # Splits, trim spaties, maak lowercase, en filter lege strings
    RELEVANT_EXTENSIONS = [
        ext.strip().lower()
        for ext in RELEVANT_EXTENSIONS_STR.split(',')
        if ext.strip() and ext.strip().startswith('.')  # Moet beginnen met een punt
    ]
    EXTRACT_NESTED_ZIPS = config.getboolean('ZipHandling', 'extract_nested_zips', fallback=False)

except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
    print(f"Fout bij het lezen van config file '{config_path}': {e}")
    sys.exit(1)
except Exception as e:
    print(f"Onverwachte fout bij het laden van de configuratie: {e}")
    sys.exit(1)


# --- Logging Instellen ---
def setup_logging():
    """Configureert logging naar console en bestand."""
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)  # Zorg dat de log map bestaat
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)  # Ook naar console
            ],
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.info(f"Logging geïnitialiseerd. Logbestand: {LOG_FILE}")
    except Exception as e:
        print(f"Kritieke fout bij opzetten logging naar {LOG_FILE}: {e}")
        sys.exit(1)


# --- Pipeline Stappen ---

def detect_new_files(source_dir):
    """Detecteert ZIP-bestanden in de bronmap."""
    logging.info(f"Zoeken naar ZIP-bestanden in: {source_dir}")
    if not source_dir.is_dir():
        # Loggen en error gooien is beter dan alleen printen
        logging.error(f"Bronmap {source_dir} niet gevonden of geen map.")
        raise FileNotFoundError(f"Bronmap {source_dir} niet gevonden of geen map.")

    files_to_process = [f for f in source_dir.iterdir() if f.is_file() and f.suffix.lower() == '.zip']
    logging.info(f"{len(files_to_process)} ZIP-bestand(en) gevonden om te verwerken.")
    return files_to_process


def copy_to_staging(file_path, staging_dir):
    """Kopieert het input ZIP-bestand naar de staging map."""
    logging.info(f"Kopiëren input ZIP '{file_path.name}' naar staging: {staging_dir}")
    staging_dir.mkdir(parents=True, exist_ok=True)
    try:
        dest_path = staging_dir / file_path.name
        # Overschrijven voorkomen in staging? Kan nuttig zijn bij hertesten.
        if dest_path.exists():
            # Voeg timestamp toe om uniek te maken
            timestamp = int(time.time())
            dest_path = staging_dir / f"{file_path.stem}_{timestamp}{file_path.suffix}"
            logging.warning(f"Bestand '{file_path.name}' bestond al in staging. Gekopieerd als '{dest_path.name}'.")

        shutil.copy2(file_path, dest_path)  # copy2 behoudt metadata
        logging.info(f"Input ZIP succesvol gekopieerd naar '{dest_path}'")
        return dest_path
    except Exception as e:
        logging.error(f"Fout bij kopiëren input ZIP '{file_path.name}' naar {staging_dir}: {e}")
        raise IOError(f"Fout bij kopiëren input ZIP '{file_path.name}' naar {staging_dir}: {e}")


def extract_zip_in_staging(staged_zip_path, staging_dir):
    """Pakt een ZIP-bestand uit in een unieke submap binnen staging."""
    zip_filename_stem = staged_zip_path.stem
    # Gebruik timestamp voor gegarandeerd unieke extractiemap
    timestamp = int(time.time())
    extract_subdir = staging_dir / f"extracted_{zip_filename_stem}_{timestamp}"

    logging.info(f"Start uitpakken van '{staged_zip_path.name}' naar '{extract_subdir.relative_to(staging_dir)}'")
    extracted_files_paths = []
    try:
        extract_subdir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(staged_zip_path, 'r') as zip_ref:
            # Controleer op corrupte bestanden tijdens extractie
            corrupt_files = zip_ref.testzip()
            if corrupt_files:
                logging.warning(f"ZIP '{staged_zip_path.name}' bevat mogelijk corrupte bestanden: {corrupt_files}")
                # Je kunt hier kiezen om te stoppen of door te gaan

            zip_ref.extractall(extract_subdir)
            # Verzamel paden van alle direct uitgepakte bestanden
            for item in extract_subdir.rglob('*'):
                if item.is_file():
                    extracted_files_paths.append(item)
        logging.info(
            f"ZIP succesvol uitgepakt. {len(extracted_files_paths)} bestanden gevonden in '{extract_subdir.relative_to(staging_dir)}'.")

        # --- Geneste ZIPs (momenteel uitgeschakeld via config) ---
        if EXTRACT_NESTED_ZIPS:
            # (Code voor geneste extractie zou hier komen, zoals in vorige versie)
            logging.warning(
                "Extractie van geneste ZIPs is ingeschakeld maar code is hier niet actief geïmplementeerd in deze versie.")
            pass  # Voor nu niets doen

        # Filter op relevante extensies (indien opgegeven in config)
        if RELEVANT_EXTENSIONS:
            files_to_upload = [
                f for f in extracted_files_paths
                if f.suffix.lower() in RELEVANT_EXTENSIONS
            ]
            logging.info(
                f"{len(files_to_upload)} relevante bestanden gevonden na filteren op extensies: {', '.join(RELEVANT_EXTENSIONS)}")
        else:
            # Als RELEVANT_EXTENSIONS leeg is, upload alles
            files_to_upload = extracted_files_paths
            logging.info(
                f"Geen specifieke extensies opgegeven, {len(files_to_upload)} bestanden gevonden om te uploaden.")

        # Sluit .zip bestanden uit van upload als nested extractie UIT staat (om te voorkomen dat je de zips zelf upload)
        if not EXTRACT_NESTED_ZIPS:
            files_to_upload = [f for f in files_to_upload if f.suffix.lower() != '.zip']
            logging.debug(
                f"{len(files_to_upload)} bestanden blijven over na uitsluiten van .zip (omdat nested extractie uit staat).")

        return extract_subdir, files_to_upload

    except zipfile.BadZipFile:
        logging.error(f"Input ZIP bestand '{staged_zip_path.name}' is corrupt of geen geldig ZIP-bestand.")
        # Probeer de (mogelijk deels) uitgepakte map te verwijderen
        if extract_subdir.exists():
            try:
                shutil.rmtree(extract_subdir)
            except OSError as rm_err:
                logging.error(f"Kon mislukte extractie map '{extract_subdir}' niet verwijderen: {rm_err}")
        raise ValueError(f"Input ZIP bestand '{staged_zip_path.name}' is corrupt.")
    except Exception as e:
        logging.error(f"Fout bij uitpakken ZIP '{staged_zip_path.name}': {e}")
        # Probeer op te ruimen
        if extract_subdir.exists():
            try:
                shutil.rmtree(extract_subdir)
            except OSError as rm_err:
                logging.error(f"Kon mislukte extractie map '{extract_subdir}' niet verwijderen: {rm_err}")
        raise IOError(f"Fout bij uitpakken ZIP '{staged_zip_path.name}': {e}")


def publish_extracted_files_to_ckan(original_zip_name, extract_subdir, files_to_upload, ckan_url, api_key, owner_org):
    """Publiceert/update relevante uitgepakte bestanden naar CKAN."""
    logging.info(f"Start CKAN publicatie voor inhoud van '{original_zip_name}'")

    # Dataset ID afleiden van *originele* ZIP bestandsnaam
    base_name = Path(original_zip_name).stem
    # Maak een CKAN-vriendelijke ID (lowercase, koppeltekens)
    dataset_id = ''.join(c if c.isalnum() else '-' for c in base_name).lower().strip('-')
    # Titel mag leesbaarder zijn
    dataset_title = base_name.replace('_', ' ').replace('-', ' ').title()

    # Voorkom lege dataset_id
    if not dataset_id:
        dataset_id = f"dataset-{int(time.time())}"
        logging.warning(
            f"Kon geen geldige dataset ID afleiden van '{original_zip_name}', gebruik gegenereerde ID: '{dataset_id}'")

    logging.info(f"Doel CKAN Dataset: ID='{dataset_id}', Titel='{dataset_title}'")

    if not files_to_upload:
        logging.warning(
            f"Geen bestanden gevonden in '{extract_subdir.name}' om te publiceren naar CKAN voor dataset '{dataset_id}'.")
        return  # Geen werk te doen

    # Gebruik een sessie voor mogelijk betere performance/reuse van connectie
    ckan_error = False
    package = None
    try:
        ckan = RemoteCKAN(ckan_url, apikey=api_key)

        # Stap 1: Check/Create dataset (package)
        try:
            logging.debug(f"Controleren of dataset '{dataset_id}' bestaat...")
            package = ckan.call_action('package_show', {'id': dataset_id})
            logging.info(f"Dataset '{dataset_id}' (ID: {package['id']}) bestaat al.")
            # Update dataset metadata? Bv. 'laatst bijgewerkt' notitie
            try:
                update_notes = package.get('notes', '').split('\nLaatst bijgewerkt:')[0]  # Oude notitie behouden
                update_notes += f'\nLaatst bijgewerkt met inhoud van {original_zip_name} op {time.strftime("%Y-%m-%d %H:%M:%S")}'
                ckan.call_action('package_patch', {'id': package['id'], 'notes': update_notes})
                logging.info(f"Dataset '{dataset_id}' metadata bijgewerkt.")
            except Exception as patch_err:
                logging.warning(f"Kon metadata van dataset '{dataset_id}' niet bijwerken: {patch_err}")

        except NotFound:
            logging.info(f"Dataset '{dataset_id}' bestaat nog niet. Aanmaken...")
            try:
                package_data = {
                    'name': dataset_id,
                    'title': dataset_title,
                    'owner_org': owner_org,
                    'notes': f'Dataset automatisch aangemaakt voor ZIP-bestand {original_zip_name} op {time.strftime("%Y-%m-%d %H:%M:%S")}',
                    # Voeg eventueel meer standaard metadata toe
                    'author': 'Data Pipeline Script',
                }
                package = ckan.call_action('package_create', package_data)
                logging.info(f"Dataset '{dataset_id}' (ID: {package['id']}) succesvol aangemaakt.")
            except ValidationError as e:
                # Log specifieke validatiefouten indien mogelijk
                error_details = e.error_dict if hasattr(e, 'error_dict') else str(e)
                logging.error(f"CKAN Validatiefout bij aanmaken dataset '{dataset_id}': {error_details}")
                ckan_error = True
            except Exception as e:
                logging.error(f"Fout bij aanmaken CKAN dataset '{dataset_id}': {e}")
                ckan_error = True
        except NotAuthorized as e:
            logging.error(
                f"CKAN Autorisatiefout bij controleren/aanmaken dataset '{dataset_id}'. Controleer API key en rechten voor org '{owner_org}'. Fout: {e}")
            ckan_error = True
        except Exception as e:
            logging.error(f"Algemene fout bij controleren/aanmaken CKAN dataset '{dataset_id}': {e}")
            ckan_error = True

        if ckan_error or not package or 'id' not in package:
            raise ConnectionError(
                f"Kon CKAN dataset '{dataset_id}' niet vinden of aanmaken. Stoppen met publiceren voor deze ZIP.")

        package_id = package['id']  # We hebben de ID nodig

        # Stap 2: Haal bestaande resources op voor efficiënte check
        existing_resources = {}  # {resource_name: resource_id}
        try:
            # Gebruik package_show ipv package_search voor betrouwbaardere resultaten
            package_details = ckan.call_action('package_show', {'id': package_id})
            for res in package_details.get('resources', []):
                # Soms is 'name' None, skip die resources
                if res.get('name'):
                    existing_resources[res['name']] = res['id']
            logging.info(f"Dataset '{dataset_id}' heeft {len(existing_resources)} bestaande resource(s) met een naam.")
        except Exception as e:
            logging.warning(
                f"Kon bestaande resources voor dataset '{dataset_id}' niet ophalen: {e}. Update check gebeurt per bestand.")
            existing_resources = {}  # Reset om fallback te forceren

        # Stap 3: Verwerk elk relevant uitgepakt bestand
        successful_uploads = 0
        failed_uploads = 0
        for file_path in files_to_upload:
            # Resource naam moet uniek zijn binnen dataset, gebruik bestandsnaam
            # CKAN kan moeite hebben met speciale tekens, normaliseer eventueel
            resource_name = file_path.name
            relative_path = file_path.relative_to(extract_subdir)  # Pad binnen de extractie map
            resource_description = f"Bestand '{relative_path}' uit {original_zip_name}, verwerkt op {time.strftime('%Y-%m-%d %H:%M:%S')}"
            resource_format = file_path.suffix.lstrip('.').upper() if file_path.suffix else 'Unknown'

            logging.info(f"Verwerken bestand voor CKAN: '{relative_path}' (Resource naam: '{resource_name}')")

            try:
                # Gebruik 'with open' voor correct sluiten van bestand
                with open(file_path, 'rb') as file_object:
                    resource_data = {
                        'package_id': package_id,
                        'name': resource_name,
                        'description': resource_description,
                        'format': resource_format,
                        'upload': file_object,  # Het file object zelf
                        # 'url' is niet nodig bij upload, CKAN genereert die
                    }

                    # Check of resource al bestaat (efficiënt via opgehaalde lijst, anders via API call)
                    existing_resource_id = existing_resources.get(resource_name)
                    action = None
                    if existing_resource_id:
                        logging.debug(
                            f"Resource '{resource_name}' gevonden in cache (ID: {existing_resource_id}). Voorbereiden voor update.")
                        resource_data['id'] = existing_resource_id
                        action = 'resource_update'
                    else:
                        # Als niet in cache, probeer create (kan falen als hij toch bestaat -> niet waterdicht zonder cache)
                        logging.debug(f"Resource '{resource_name}' niet in cache. Voorbereiden voor create.")
                        action = 'resource_create'

                    # Voer actie uit
                    if action == 'resource_update':
                        logging.info(f"Updaten bestaande resource '{resource_name}'...")
                        updated_resource = ckan.call_action(action, resource_data)
                        logging.info(f"Resource '{resource_name}' (ID: {updated_resource['id']}) succesvol bijgewerkt.")
                        successful_uploads += 1
                    elif action == 'resource_create':
                        logging.info(f"Aanmaken nieuwe resource '{resource_name}'...")
                        new_resource = ckan.call_action(action, resource_data)
                        logging.info(f"Resource '{resource_name}' (ID: {new_resource['id']}) succesvol aangemaakt.")
                        successful_uploads += 1

            except FileNotFoundError:
                logging.error(
                    f"Bestand '{file_path}' niet gevonden tijdens CKAN upload poging (is het tussentijds verwijderd?).")
                failed_uploads += 1
            except ValidationError as e:
                error_details = e.error_dict if hasattr(e, 'error_dict') else str(e)
                logging.error(f"CKAN Validatiefout voor resource '{resource_name}': {error_details}")
                failed_uploads += 1
            except NotAuthorized as e:
                logging.error(
                    f"CKAN Autorisatiefout voor resource '{resource_name}'. Controleer API key rechten. Fout: {e}")
                failed_uploads += 1
                # Mogelijk hier stoppen als authenticatie faalt?
                # raise ConnectionError("CKAN Autorisatiefout, stoppen.")
            except Exception as e:
                # Vang andere CKAN of netwerkfouten
                logging.error(f"Algemene fout bij verwerken resource '{resource_name}' naar CKAN: {e}",
                              exc_info=False)  # exc_info=False om dubbele traceback te voorkomen bij logging
                logging.debug("Traceback voor resource fout:", exc_info=True)  # Log traceback naar bestand
                failed_uploads += 1
                # Optioneel: stop bij de eerste resource fout?
                # raise ConnectionError(f"Fout bij resource '{resource_name}', stoppen met uploads voor deze ZIP.")

        logging.info(
            f"CKAN publicatie voor '{original_zip_name}' voltooid. Succes: {successful_uploads}, Fouten: {failed_uploads}.")
        if failed_uploads > 0:
            # Gooi een fout zodat de main loop weet dat er iets misging met deze ZIP
            raise ConnectionError(
                f"{failed_uploads} resource(s) konden niet worden gepubliceerd naar CKAN voor '{original_zip_name}'.")


    except Exception as e:
        logging.error(f"Kritieke fout tijdens CKAN publicatie proces voor '{original_zip_name}': {e}")
        raise  # Gooi fout opnieuw op voor hoofd error handling


def cleanup_staging(staged_zip_path, extract_subdir):
    """Verwijdert het gestagede ZIP-bestand en de uitgepakte map."""
    logging.info(f"Start opruimen staging area voor '{staged_zip_path.name if staged_zip_path else 'onbekend ZIP'}'")
    if staged_zip_path and staged_zip_path.exists():
        try:
            staged_zip_path.unlink()
            logging.info(f"Gestaged ZIP bestand '{staged_zip_path.name}' verwijderd.")
        except OSError as e:
            logging.warning(f"Kon gestaged ZIP bestand '{staged_zip_path.name}' niet verwijderen: {e}")
    else:
        logging.debug("Geen gestaged ZIP pad opgegeven of bestand bestaat niet meer.")

    if extract_subdir and extract_subdir.exists():
        try:
            # Wees voorzichtig met rmtree! Dubbelcheck pad.
            if STAGING_DIR in extract_subdir.parents:  # Extra check dat we binnen staging blijven
                shutil.rmtree(extract_subdir)
                logging.info(f"Extractie map '{extract_subdir.name}' verwijderd.")
            else:
                logging.error(
                    f"WEIGEREN te verwijderen: Map '{extract_subdir}' lijkt buiten de staging dir '{STAGING_DIR}' te vallen.")
        except OSError as e:
            logging.warning(f"Kon extractie map '{extract_subdir.name}' niet verwijderen: {e}")
    else:
        logging.debug("Geen extractie submap opgegeven of map bestaat niet meer.")


def move_to_processed(file_path, processed_dir):
    """Verplaatst het originele input ZIP-bestand naar de 'verwerkt' map."""
    # Deze functie wordt alleen aangeroepen als MOVE_PROCESSED True is en PROCESSED_DIR is ingesteld.
    logging.info(f"Verplaatsen origineel input ZIP bestand '{file_path.name}' naar: {processed_dir}")
    try:
        processed_dir.mkdir(parents=True, exist_ok=True)
        dest_path = processed_dir / file_path.name
        # Voorkom overschrijven - voeg timestamp toe als bestand al bestaat
        counter = 1
        original_dest_path = dest_path
        while dest_path.exists():
            timestamp = int(time.time())
            dest_path = processed_dir / f"{file_path.stem}_{timestamp}_{counter}{file_path.suffix}"
            counter += 1
        if dest_path != original_dest_path:
            logging.warning(
                f"Bestand '{original_dest_path.name}' bestond al in {processed_dir}. Hernoemd naar '{dest_path.name}' bij verplaatsen.")

        shutil.move(str(file_path), str(dest_path))  # shutil.move werkt beter met strings voor paden
        logging.info(f"Origineel input ZIP bestand succesvol verplaatst naar '{dest_path}'")
    except Exception as e:
        # Fout bij verplaatsen mag de rest niet blokkeren, maar moet gelogd worden.
        logging.error(
            f"Fout bij verplaatsen origineel ZIP bestand '{file_path.name}' naar {processed_dir}: {e}. Bestand blijft in bronmap '{SOURCE_DIR}' staan.")
        # Geen e-mail meer, alleen loggen.


# --- Hoofd Pipeline Logica ---
def main():
    """Voert de volledige data pipeline uit voor alle ZIPs in de source_dir."""
    setup_logging()  # Start logging zo snel mogelijk
    logging.info("=" * 50)
    logging.info("--- Start Data Pipeline Run (ZIP Processing) ---")
    logging.info(f"Bronmap: {SOURCE_DIR}")
    logging.info(f"Stagingmap: {STAGING_DIR}")
    logging.info(f"Verplaats verwerkte bestanden: {MOVE_PROCESSED}")
    if MOVE_PROCESSED and PROCESSED_DIR:
        logging.info(f"Verwerkt map: {PROCESSED_DIR}")
    logging.info(f"CKAN URL: {CKAN_URL}")
    logging.info(f"CKAN Owner Org: {CKAN_OWNER_ORG}")
    logging.info(f"Upload alleen extensies: {'Alle' if not RELEVANT_EXTENSIONS else ', '.join(RELEVANT_EXTENSIONS)}")
    logging.info(f"Pak geneste ZIPs uit: {EXTRACT_NESTED_ZIPS}")
    logging.info("=" * 50)

    start_time = time.time()
    total_processed_zips = 0
    total_error_zips = 0

    try:
        # 1. Detecteer nieuwe input ZIP bestanden
        zip_files_to_process = detect_new_files(SOURCE_DIR)

        if not zip_files_to_process:
            logging.info("Geen nieuwe ZIP-bestanden gevonden om te verwerken.")
        else:
            logging.info(f"Start verwerking van {len(zip_files_to_process)} ZIP bestand(en)...")

        # Verwerk elk gevonden ZIP bestand
        for i, source_zip_path in enumerate(zip_files_to_process, 1):
            zip_start_time = time.time()
            original_zip_name = source_zip_path.name
            logging.info(f"--- [{i}/{len(zip_files_to_process)}] Start verwerking ZIP: '{original_zip_name}' ---")

            staged_zip_path = None
            extract_subdir = None
            zip_processed_successfully = False  # Flag om te bepalen of verplaatsen mag

            try:
                # 2. Kopieer input ZIP naar Staging
                staged_zip_path = copy_to_staging(source_zip_path, STAGING_DIR)

                # 3. Pak ZIP uit in Staging
                extract_subdir, relevant_files_to_upload = extract_zip_in_staging(staged_zip_path, STAGING_DIR)

                # 4. Publiceer relevante uitgepakte bestanden naar CKAN
                publish_extracted_files_to_ckan(
                    original_zip_name,
                    extract_subdir,
                    relevant_files_to_upload,
                    CKAN_URL,
                    CKAN_API_KEY,
                    CKAN_OWNER_ORG
                )

                # Als we hier komen zonder exceptions, is deze ZIP succesvol verwerkt
                zip_processed_successfully = True
                total_processed_zips += 1
                logging.info(f"Input ZIP '{original_zip_name}' succesvol verwerkt.")


            except (FileNotFoundError, IOError, ValueError, ConnectionError, zipfile.BadZipFile, Exception) as e:
                total_error_zips += 1
                # Log de fout specifiek voor deze ZIP
                logging.error(f"Pipeline FOUT voor input ZIP '{original_zip_name}': {e}", exc_info=False)
                # De volledige traceback wordt al gelogd binnen de falende functie (als debug) of hier als nodig
                logging.debug(f"Traceback voor fout bij '{original_zip_name}':", exc_info=True)
                # Ga door met het volgende ZIP bestand

            finally:
                # 5. Ruim staging area op (altijd proberen, ongeacht succes/falen)
                cleanup_staging(staged_zip_path, extract_subdir)

                # 6. (Optioneel) Verplaats origineel input ZIP bestand na succes
                # Alleen verplaatsen als geconfigureerd EN deze specifieke zip succesvol was
                if MOVE_PROCESSED and PROCESSED_DIR and zip_processed_successfully:
                    move_to_processed(source_zip_path, PROCESSED_DIR)
                elif MOVE_PROCESSED and not zip_processed_successfully:
                    logging.warning(
                        f"Origineel bestand '{original_zip_name}' wordt NIET verplaatst naar processed dir vanwege fouten tijdens verwerking.")

            zip_duration = time.time() - zip_start_time
            logging.info(
                f"--- [{i}/{len(zip_files_to_process)}] Einde verwerking ZIP: '{original_zip_name}'. Duur: {zip_duration:.2f} sec. ---")


    except FileNotFoundError as e:
        # Fout bij het vinden van de source dir is kritiek
        logging.critical(f"Kritieke fout bij toegang tot bronmap: {e}. Pipeline stopt.", exc_info=True)
        total_error_zips += 1
    except Exception as e:
        # Vang andere onverwachte kritieke fouten in de hoofdloop
        logging.critical(f"Onverwachte kritieke fout in pipeline hoofdloop: {e}", exc_info=True)
        total_error_zips += 1

    finally:
        # Log samenvatting van de totale run
        end_time = time.time()
        total_duration = end_time - start_time
        logging.info("=" * 50)
        logging.info("--- Einde Data Pipeline Run (ZIP Processing) ---")
        logging.info(
            f"Totaal aantal ZIPs gevonden: {len(zip_files_to_process) if 'zip_files_to_process' in locals() else 0}")
        logging.info(f"Totaal succesvol verwerkte ZIPs: {total_processed_zips}")
        logging.info(f"Totaal aantal ZIPs met fouten: {total_error_zips}")
        logging.info(f"Totale duur van de run: {total_duration:.2f} seconden.")
        logging.info("=" * 50)


# Zorgt dat main() alleen draait als het script direct wordt uitgevoerd
if __name__ == "__main__":
    main()