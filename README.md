[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](LICENSE)

# Scicat Ingestor

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

## Developer's Guide

### Virtual Environments
We use [pip-compile-multi](https://pip-compile-multi.readthedocs.io/en/latest/) to maintain
the `pip` recipes under `requirements`.
Each `*.in` files have the dependencies for specific purposes and `*.txt` files are compiled frozen dependencies.
`base.in` is auto-generated by `make_base.py` script based on `pyproject.toml`,
but you can add/pin dependencies manually above the marker in the file.

We use [tox](#Tox) command, `tox -e deps` to freeze the dependencies.

### Pre-commit Hooks
We use `pre-commit` for formatting and static analysis of the code.
Once you clone the repository and set up the virtual environment (i.e. installed `requirements/dev.txt`)
you need to install [pre-commit](https://pre-commit.com/index.html) by
```bash
pre-commit install
```
`pre-commit` will be hooked whenever you make a `git commit` and screen the files of interest.

It will not pass CI test if `pre-commit` complains.
If you want to push the change while having a PR open, even if you know that `pre-commit hooks` fail,
you can add `[ci-skip]` tag in your commit and CI action will be skipped.

### Copier Template
This repository was based on [scipp/copier_template](https://github.com/scipp/copier_template).
We can apply common updates/dependency updates using
```bash
copier update
```

### Release
We use github release to create a new tag and release notes.
Typically we do not include dependabot PRs from the auto generated release notes.

Release versions are [calendar version](https://calver.org/).
For example, if it is 27.01.2025 and if it is the first version of this month, the version will be ``v25.01.0``.
If another release or a patch release is made within the same month,
the minor version can increase, i.e. ``v25.01.1``.

### Tox
`tox` controls virtual environment and commands for various purposes.
Developers and CI actions can use the command.
For example, `tox -e docs` builds documentation under `./html` directory and `tox -e py310` will run unit tests with python version `3.10`.
