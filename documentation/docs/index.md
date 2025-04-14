# Welcome to Scicat Ingestor

Scicat ingestor creates a raw dataset along with metadata using
``wrdn`` messages and scicat api whenever a new file is written by a file-writer.

## How to INSTALL
```bash
git clone https://github.com/SciCatProject/scicat-ingestor.git
cd scicat-ingestor
pip install -e .  # It will allow you to use entry-points of the scripts,
                  # defined in ``pyproject.toml``, under ``[project.scripts]`` section.
```

## How to RUN

All commands have prefix of ``scicat`` so that you can use auto-complete in a terminal.

Each command is connected to a free function in a module. It is defined in ``pyproject.toml``, under ``[project.scripts]`` section.

All scripts parse the system arguments and configuration in the same way.

### Online ingestor (Highest level interface)
You can start the ingestor daemon with certain configurations.

It will continuously process `wrdn` messages and ingest the corresponding nexus files.

```bash
scicat_ingestor --logging.verbose -c PATH_TO_CONFIGURATION_FILE.yaml
```

**A topic can contain non-`wrdn` message so the ingestor filters messages and ignores irrelevant types of messages.**

See [configuration](#configuration) for how to use configuration files.

### Background ingestor  (Lower level interface)
You can also run the ingestor file by file.

You need to know the path to the nexus file you want to ingest
and also the path to the ``done_writing_message_file`` as a json file.

```bash
scicat_background_ingestor \
    --logging.verbose \
    -c PATH_TO_CONFIGURATION_FILE.yaml \
    --nexus-file PATH_TO_THE_NEXUS_FILE.nxs \
    --done-writing-message-file PATH_TO_THE_MESSAGE_FILE.json
```

### Dry run

You can add ``--ingestion.dry-run`` flag for dry-run testings.

```bash
scicat_ingestor --logging.verbose -c PATH_TO_CONFIGURATION_FILE.yaml --ingestion.dry-run
```

```bash
scicat_background_ingestor \
    --logging.verbose \
    -c PATH_TO_CONFIGURATION_FILE.yaml \
    --nexus-file PATH_TO_THE_NEXUS_FILE.nxs \
    --done-writing-message-file PATH_TO_THE_MESSAGE_FILE.json \
    --ingestion.dry-run
```

## Configuration

You can use a json file to configure options.
There is a template, ``resources/config.sample.json`` you can copy/paste to make your own configuration file.

In order to update the configurations, you should update it the ``scicat_configuration`` module.

The template file can be synchronized automatically by ``scicat_synchronize_config`` command.

**There is a unit test that checks if the online ingestor configuration dataclass is in sync with the ``resources/config.sample.json``.**

### Configuration Validator

You can validate a configuration file with ``scicat_validate_ingestor_config`` command.

```bash
scicat_validate_ingestor_config
```

It tries building nested configuration dataclasses from the configuration file.

It will throw errors if configuration is invalid.

i.e. In the operation, it'll ignore extra keywords that do not match the configuration dataclass arguments
but validator throws an error if there are extra keywords that do not match the arguments.

This is part of CI tests.
