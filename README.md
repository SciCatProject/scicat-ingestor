[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](LICENSE)

# Scicat Filewriter Ingest

## About

A daemon that creates a raw dataset using scicat interface whenever a new file is written by a file-writer.

## Configuration

You can use a json file to configure options.
There is a template, ``resources/config.sample.json`` you can copy/paste.

```bash
cp resources/config.sample.json config.20240405.json
```

Then ``scicat_ingestor`` will automatically use the configuration file.
