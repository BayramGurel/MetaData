# MetaData

This repository contains a small example for extracting metadata from a ZIP
archive using Apache Tika. The Java implementation writes the results to
`report.json`. A helper Python script can upload the results to a CKAN
instance.

## Requirements

* Java 17
* Maven (for building the Java project)
* Python 3 with `ckanapi` and `tqdm` (optional, for uploading)

## Building

Use Maven to compile the project:

```bash
mvn package
```

This will compile the classes under `src` and place the output in
`target/classes`.

## Running the extractor

The extractor expects the archive `document/Veg kartering - habitatkaart 2021-2023.zip`
to be present. Run it with:

```bash
java -cp target/classes:<dependencies> MetadataExtractor
```

A `report.json` file will be generated with metadata for the files inside the
archive.

## Uploading to CKAN

The script `python/upload_resources.py` reads `report.json` and interacts with a
CKAN instance. Install its dependencies and run:

```bash
pip install ckanapi tqdm
python python/upload_resources.py
```