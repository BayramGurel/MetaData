# Metadata example

This repository showcases a small workflow for extracting metadata from the
files inside a ZIP archive using **Apache&nbsp;Tika**. The Java code outputs its
results to `report.json`. A companion Python script can then upload these
results to a CKAN instance.

## Requirements

* Java 17
* Maven (for building the Java project)
* Python 3 with `ckanapi` and `tqdm` (optional, for uploading)

## Building

The project uses Maven. From the repository root run:

```bash
mvn package
```

This downloads all dependencies and compiles the classes under `src` into
`target/classes`.

## Running the extractor

The example archive `document/Veg kartering - habitatkaart 2021-2023.zip`
is included in the repository. After building, execute the extractor:

```bash
java -cp target/classes:<dependencies> MetadataExtractor [path]
```

The optional `path` argument can point to a file or directory. If it is a file
(for example the included ZIP archive) the
results are written to `report.json`. When `path` is a directory every regular
file inside that directory is processed individually and a file named
`report-<filename>.json` will be created for each one.

### Running from IntelliJ

1. Open the repository as a **Maven** project.
2. IntelliJ automatically downloads the dependencies and compiles the sources.
3. Locate `MetadataExtractor.java` and press **Run**. The IDE handles the class
   path for you and places the results in `report.json`.

### Running from Visual Studio Code

Visual Studio Code does not compile Maven projects automatically. Use the
terminal to build first:

```bash
mvn package
```

Afterwards open `MetadataExtractor.java` and invoke **Run Java** or execute the
`java` command shown above from the integrated terminal.

## Uploading to CKAN

`python/upload_resources.py` reads `report.json` and sends the metadata to a
CKAN instance. At the top of the script you can adjust the `CKAN_URL` and
`API_KEY` constants. Install the required Python packages and run it:

```bash
pip install ckanapi tqdm
python python/upload_resources.py
```

The script will ask a few questions (for example whether to create a new
organisation or dataset) and then upload all discovered resources.
