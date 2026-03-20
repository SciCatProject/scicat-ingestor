# Getting Started - Development

## TL;DR

``` bash
git clone git@github.com:SciCatProject/scicat-ingestor.git
pixi install
pre-commit install  # or prek install
```

## Git

If you are not ``scicat-ingestor`` maintainors, <br>
you need to either fork the repository to your own organization of private account and create a PR from there.

``` bash
git clone git@github.com:SciCatProject/scicat-ingestor.git
```

## Virtual Environment
``scicat-ingestor`` is a python project.
We use pixi to organize various environments and dev tools.

Pixi `default` environment has `scicat-ingestor` modules in editable mode and its dependencies.
Therefore you can run any `scicat-ingestor` command like below:

```bash
pixi run scicat_synchronize_config
pixi run scicat_ingestor
```

> Running a pixi task for the first time may take longer if the dependencies were not already installed.

## Pre-commit Hook

``` bash
pre-commit install

# or

prek install
```

!!! note
    Pre commit hooks are configured in `.pre-commit-config.yaml` file but some of `ruff` configurations are in `pyproject.toml`.

## Testings

We have unit tests using ``pytest`` that can be run fast and often.

``pixi run test`` command will run pytest with pre-defined configuration in a virtual environment.

There are also integration test in github ci action.

### Integration Test

In the `.github/workflows/integration.yml` file, we set up minimum docker environment (i.e. kafka broker) and try running the `scicat_ingestor`.

The test is included in the `ci.yml`, which runs for every pull request changes.

## Other DevOps Routines

### Copier Update

Copier template is already set up in ``.copier-answers.yml`` so you just need to update it once in a while.


!!! note
    Currently the python project copier template is under active modification.
    It is due to changing the dev tools from `tox` and `conda` to `pixi`.


``` bash
copier update
```

It will ask a lot of questions and most of them usually stay the same.

Here are some properties of the project that should be updated by copier:

    - pixi.toml configuration
    - pyproject.toml configuration
    - github action versions
    - project name / contact
    - CI actions
    ...

### Lock Dependencies

``` bash
pixi update
```
This command will update all environments in the ``pixi.lock`` file and install them in the virtual environment.

Once you create the lock file, push it to the project into a separate branch and create a PR to main branch.

Base and extra ependencies are parsed from `pyproject.toml` project dependencies.

## Dev Tools/Commands Overview

| Tool/Command | Configuration File | Description |
| ------------ | ------------------ | ----------- |
| ``pre-commit`` or ``prek`` | .pre-commit-config.yaml | Pre-commit hooks including linter checks.<br>Once it's set up, it will be run automatically whenever a new commit is created. It is also run by one of CI actions.<br><details><summary>Bypass Pre Commit Check</summary>You can skip pre-commit checks with ``--no-verify`` flag: ``git commit --no-verify``. <br>But please keep it passing as much as possible, as it is one of blocking CI tests.</details> |
| ``copier`` | .copier-answers.yaml | This project copies from a [``pixi copier template``](https://github.com/scipp/copier_template/)<br>You have to manually update from copier template once in a while.<br>See [Copier Update](#copier-update) for more explanation. |
| ``pixi update``| **pyproject.toml**<br> - [dependencies]<br> - [project.optional-dependencies]<br>**pixi.toml**<br> - [dependencies]<br> - [pypi-dependencies]<br> - [feature.test.pypi-dependencies] | Create lock files with dependencies. |
| ``pixi lint`` | .pre-commit-config.yaml, pyproject.toml[tool.ruff.*] | Run all pre-commit hooks on all files including ``ruff`` linting. |
| ``pixi run test`` | pixi.toml | Run pytests(unit tests).<br>You can pass more arguments to pytest. |
| ``pixi run docs`` | pixi.toml | Build documentation site. |
