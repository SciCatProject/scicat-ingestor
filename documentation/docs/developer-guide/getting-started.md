# Getting Started - Development

## TL;DR

``` bash
git clone git@github.com:SciCatProject/scicat-ingestor.git
conda create -n scicat-ingestor-dev python=3.12
conda activate scicat-ingestor-dev
pip install -r requirements/dev.txt
pre-commit install
pip install -e .
```

## Git

If you are not ``scicat-ingestor`` maintainors, <br>
you need to either fork the repository to your own organization of private account and create a PR from there.

``` bash
git clone git@github.com:SciCatProject/scicat-ingestor.git
```

## Virtual Environment
``scicat-ingestor`` is a python project.
We make multiple lock files for various environments and dev tools.

``requirements/dev.txt`` contains all dependencies for development and tools.

``` bash
conda create -n scicat-ingestor-dev python=3.12  # One and only supported version by scicat ingestor.
conda activate scicat-ingestor-dev
pip install -r requirements/dev.txt
pip install -e .
```

> The rest of the instruction will assume that this virtual environment is activated.

## Pre-commit Hook

``` bash
pre-commit install
```

!!! note
    Pre commit hooks are configured in `.pre-commit-config.yaml` file but some of `ruff` configurations are in `pyproject.toml`.

## Testings

We have unit tests using ``pytest`` that can be run fast and often.

``tox -e py312`` command will run pytest with pre-defined configuration in a virtual environment created with `requirements/test.txt`.

There are also integration test in github ci action.

<!--TODO: Fill explanation about integration test.-->

## Other DevOps Routines

### Copier Update

Copier template is already set up in ``.copier-answers.yml`` so you just need to update it once in a while.

``` bash
copier update
```

It will ask a lot of questions and most of them usually stay the same.

Here are some properties of the project that should be updated by copier:

    - python version
    - project name
    - CI actions
    - requirements/make_base.py file (Please report any bugs in this file to the template repository.)


### Lock Dependencies

``` bash
tox -e deps
```
This command will compile all ``*.in`` files and create corresponding ``*.txt`` lock files under ``requirements``.

Once you create the lock files, push it to the project into a separate branch and create a PR to main branch.

Base dependencies are parsed from `pyproject.toml` project dependencies and written into ``base.in`` file.

> See `testenv:deps` section in `tox.ini` file to see what it does.


## Dev Tools/Commands Overview

| Tool/Command | Configuration File | Description |
| ------------ | ------------------ | ----------- |
| pre-commit | .pre-commit-config.yaml | Pre-commit hooks including linter checks.<br>Once it's set up, it will be run automatically whenever a new commit is created. It is also run by one of CI actions.<br><details><summary>Bypass Pre Commit Check</summary>You can skip pre-commit checks with ``--no-verify`` flag: ``git commit --no-verify``. <br>But please keep it passing as much as possible, as it is one of blocking CI tests.</details> |
| copier | .copier-answers.yaml | This project copies from [``scipp copier template``](https://github.com/scipp/copier_template/)<br>You have to manually update from copier template once in a while.<br>See [Copier Update](#copier-update) for more explanation. |
| tox  | tox.ini            | Multiple tox environment/commands for development and CI actions.<br>It creates virtual environments and use it for each commands.<br>The virtual environment files are saved under `.tox` directory. |
| ``tox -e deps``| tox.ini/[testenv:deps] | Create lock files with dependencies. |
| ``tox -e static`` | tox.ini/[testenv:static] | Run all precommit hooks on all files. |
| ``tox -e py312`` | tox.ini/[testenv] | Run pytests with python version 3.12.<br>You can pass more arguments to pytest. |
| ``tox -e mypy`` | tox.ini/[testenv:mypy] | Run static type checks with `mypy`. |
| ``tox -e docs`` | tox.ini/[testenv:docs] | Build documentation site. **This part of the copier template is overwritten since this project does not require sphinx.** |
