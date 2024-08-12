[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](LICENSE)

# Scicat Filewriter Ingest

A daemon that creates a raw dataset using scicat interface whenever a new file is written by a file-writer.

## How to INSTALL
```bash
git clone https://github.com/SciCatProject/scicat-filewriter-ingest.git
cd scicat-filewriter-ingest
pip install -e .  # It will allow you to use entry-points of the scripts,
                  # defined in ``pyproject.toml``, under ``[project.scripts]`` section.
```

## How to RUN

All scripts parse the system arguments and configuration in the same way.

### Online ingestor (Highest level interface)
You can start the ingestor daemon with certain configurations.

It will continuously process `wrdn` messages and ingest the nexus files.

```bash
scicat_ingestor --verbose -c PATH_TO_CONFIGURATION_FILE.yaml
```

See [configuration](#configuration) for how to use configuration files.

### Background ingestor  (Lower level interface)
You can also run the ingestor file by file.

You need to know the path to the nexus file you want to ingest
and also the path to the ``done_writing_message_file`` as a json file.

```bash
background_ingestor \\
    --verbose \\
    -c PATH_TO_CONFIGURATION_FILE.yaml \\
    --nexus-file PATH_TO_THE_NEXUS_FILE.nxs \\
    --done-writing-message-file PATH_TO_THE_MESSAGE_FILE.json
```

## Configuration

You can use a json file to configure options.
There is a template, ``resources/config.sample.json`` you can copy/paste to make your own configuration file.

```bash
cp resources/config.sample.json config.20240405.json
```

Then ``scicat_ingestor`` will automatically use the configuration file.

### Configuration Validator

You can validate a configuration file with ``validate_ingestor_config`` command.

```bash
validate_ingestor_config
```

It tries building nested configuration dataclasses from the configuration file.

It will throw errors if configuration is invalid.

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

### Tox
`tox` controls virtual environment and commands for various purposes.
Developers and CI actions can use the command.
For example, `tox -e docs` builds documentation under `./html` directory and `tox -e py310` will run unit tests with python version `3.10`.

## ADR
(Architecture Decision Records)

### ADR-001: Use ``dataclass`` instead of ``jinja`` or ``dict`` to create dataset/data-block instances.
We need a dict-like template to create dataset/data-block instances via scicat APIs.
#### Reason for not using ``dict``
It used to be implemented with ``dict`` but it didn't have any verifying layer so anyone could easily break the instances without noticing or causing errors in the upstream layers.
#### Reason for not using ``jinja``

``Jinja`` template could handle a bit more complicated logic within the template, i.e. ``for`` loop or ``if`` statement could be applied to the variables.
However, the dataset/data-block instances are not complicated to utilize these features of ``jinja``.
#### Reason for using ``dataclasses.dataclass`
First we did try using ``jinja`` but the dataset/data-block instances are simple enough so we replaced ``jinja`` template with ``dataclass``.
``dataclass`` can verify name and type (if we use static checks) of each field.
It can be easily turned into a nested dictionary using ``dataclasses.asdict`` function.

#### Downside of using ``dataclass`` instead of ``jinja``
With ``jinja`` template, certain fields could be skipped based on a variable.
However, it is not possible in the dataclass so it will need extra handling after turning it to a dictionary.
For example, each datafile item can have ``chk`` field, but this field shouldn't exist if checksum was not derived.
With jinja template we could handle this like below
```jinja
{
    "path": "{{ path }}",
    "size": {{ size }},
    "time": "{{ time }}",
    {% if chk %}"chk": "{{ chk }}"{% endif %}
}
```
However, with dataclass this should be handled like below.
```python
from dataclasses import dataclass, asdict
@dataclass
class DataFileItem:
    path: str
    size: int
    time: str
    chk: None | str = None

data_file_item = {
    k: v
    for k, v in asdict(DataFileItem('./', 1, '00:00')).items()
    if (k!='chk' or v is not None)
}
```
