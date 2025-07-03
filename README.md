## Metadata Extraction Workflow

This repository demonstrates how to extract file metadata from ZIP archives using **Apache Tika**, and then upload the results to a CKAN data portal.

---

### What is Apache Tika?

Apache Tika is a Java library for detecting and extracting metadata and text from over a thousand different file types (e.g., PDF, Microsoft Office, images). It is widely used, actively maintained by the Apache Software Foundation, and considered safe for analyzing both proprietary and open file formats.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Project Structure](#project-structure)
3. [Building the Java Extractor](#building-the-java-extractor)
4. [Running the Metadata Extractor](#running-the-metadata-extractor)

   * [Command Line](#running-from-command-line)
   * [IntelliJ IDEA](#running-from-intellij-idea)
   * [Visual Studio Code](#running-from-visual-studio-code)
5. [Uploading to CKAN](#uploading-to-ckan)
6. [Example](#example)
7. [License](#license)

---

## Prerequisites

* **Java Development Kit (JDK) 17**
* **Apache Maven** (used for building the Java project)
* **Python 3.7+** with the `ckanapi` and `tqdm` libraries (optional, for uploading results)
* An IDE of your choice (e.g., IntelliJ IDEA or PyCharm). IntelliJ is recommended for Java, and PyCharm for Python.

---

## Project Structure

```
├── document/
│   └── Veg kartering - habitatkaart 2021-2023.zip   # Sample archive
│   └── rest.zip                                     # zip files
├── src/
│   └── /MetadataExtractor.java                    # Java extractor
│   └── /rest.java                                 # Java extractor
├── target/                                        # Maven build output
├── reports/all-reports.json                       # Metadata output (after running)
├── python/
│   └── upload_resources.py                        # Python upload script
│
├── ckan/ckan.ini                                  # Backup from ckan.ini file
├── pom.xml                                        # Maven configuration
└── README.md                                      # This file
```

---

## Building the Java Extractor (optioneel met intelliJ IDEA)

1. Open a terminal in the repository root.
2. Run:

   ```bash
   mvn package
   ```
3. Maven will download dependencies and compile the code into `target/classes`.

> **Note:** If you open this project in IntelliJ IDEA as a Maven project, IntelliJ will handle dependency downloads and compilation automatically.

---

## Running the Metadata Extractor

The extractor processes files inside a ZIP archive (or a directory) and writes metadata output to JSON.

### The extractor processes and behavior
* **Default behavior:** If you omit the path argument, the extractor will scan all files in `document/`.
* **Single file:** Pass a path to a ZIP or any regular file to process just that file.
* **Directory:** Pass a directory path to process every file inside it.

Results:

* **Single output:** `report.json` for a single input archive/file. (effectief)
* **Multiple outputs:** For a directory, each file produces `report-<filename>.json`. (Ineffectief)

### Running from IntelliJ IDEA

1. Import the repository as a **Maven** project.
2. IntelliJ will download dependencies and compile sources.
3. Open `MetadataExtractor.java` and click **Run**.

Output is written to `report.json` by default.

### Running from Visual Studio Code

1. Open the project folder.
2. In the terminal, build with:

   ```bash
   mvn package
   ```
3. Open `MetadataExtractor.java`, then use the **Run Java** button in the editor, or execute the same `java` command as above in the integrated terminal.

---

## Uploading Metadata to CKAN

The Python script reads `report.json` (or multiple JSON files) and uploads entries to a CKAN instance.

1. Install Python dependencies:

   ```bash
   pip install ckanapi tqdm
   ```
2. Edit `python/upload_resources.py`:

   ```python
   CKAN_URL = "https://your-ckan.example.org"
   API_KEY  = "<your-api-key>"
   ```
3. Run the script:

   ```bash
   python python/upload_resources.py
   ```
4. Follow the prompts to choose or create an organization and dataset, then confirm each resource upload.

---

## Example Workflow (not tested)

1. **Build Java code:** `mvn package`
2. **Extract metadata:**

   ```bash
   java -jar target/classes MetadataExtractor document/
   ```
3. **Upload to CKAN:** `python python/upload_resources.py`
4. **Verify:** Check your CKAN portal for newly added resources.

---

## License

This project is licensed under the Province of South Holland. See the [LICENSE](LICENSE) file for details.
