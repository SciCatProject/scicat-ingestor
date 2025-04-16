# Offline Ingestor

``offline-ingestor`` is a simple command line interface.
When it is called, ``offline-ingestor`` ingests one data file according to a specific schema file (.imsc.json).

!!! tip

    If you are looking for a python interface that you can use to download/upload dataset occasionally, you should use [``scitacean``](https://www.scicatproject.org/scitacean/).

    <details>
    <summary>But... why?</summary>

    `offline-ingestor` is for `raw` datasets that are continuously produced by `daq` programs. It has very limited/low-level interface to communicate with scicat because it is expecting only certain types of files with known structure. (i.e. nexus file written by `ess-file-writer`.) Therefore it can't handle arbitrary type of files. `scitacean`, however, provides versatile high-level interfaces to process files and communicate with scicat with various authentication methods.

    `scitacean` can also validate a dataset before it is uploaded or after it is downloaded and `scitacean` will provide informative error messages.

    </details>


## Flow

```mermaid
    flowchart TB

    conf@{ shape: doc, label: "Configuration File" } --> readconfig
    nexus@{ shape: doc, label: "Data File (Nexus)" } --> selectschema
    schemas@{ shape: docs, "Schema Definition Files" } --> loadschema

    readconfig[Read the Configuration] --> loadschema[Load Schema Files] --> selectschema[Select Schema that matches the data file]
    selectschema --> schema@{ shape: doc, "Selected Schema Definition" }

```
