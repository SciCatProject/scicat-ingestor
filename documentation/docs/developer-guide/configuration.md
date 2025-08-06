# Configuration Development

As `scicat-ingestor` communicates with various frameworks via network,
there are many configurations to maintain and handle.

Therefore we decided to group them as a nested dataclass and keep the dataclass as a source of truth.<br>
Then it is exported as a json file in ``resources/config.sample.yml`` which has all possible options with their<br>
default values.
> See [Architecture Decision Records](adrs.md#adr-001-use-dataclass-instead-of-jinja-or-dict-to-create-datasetdata-block-instances) to see why.

Argument parser is also automatically built by [``scicat_configuration.build_arg_parser``](https://github.com/SciCatProject/scicat-ingestor/blob/main/src/scicat_configuration.py).

## Update Configuration

If you need to change anything in the configuration,<br>
you will have to update data classes and sync the dataclasses with the ``json`` config file.

There should be one entry configuration that contains all configurations for the scicat ingestor program.<br>
``online ingestor`` and ``offline ingestor`` use different configuration class.<br>

| online ingestor | offline ingestor |
| --------------- | ---------------- |
| OnlineIngestorConfig | OfflineIngestorConfig |

Once any configuration is updated, it should be exported to json using this command that comes with the package.

``` bash
# It calls `synchronize_config_file` function in `scicat_configuration` module.
scicat_synchronize_config
```

There is also a unit test to check if they are in sync so don't worry about forgetting them.<br>
CI will scream about it...!


## Using Configuration

Any other modules that needs configurations should **always** use the configuration dataclass object, instead of a plain dictionary.

!!! tip

    Sometimes, it is easier to have a `read-only` property in configuration dataclass object that is built as it is read based on the other configuration properties.

    For example:

    ``` python

    @dataclass(kw_only=True)
    class SciCatOptions:
        host: str = "https://scicat.host"
        token: str = "JWT_TOKEN"
        additional_headers: dict = field(default_factory=dict)
        ...

        @property
        def headers(self) -> dict:
            return {
                **self.additional_headers,
                **{"Authorization": f"Bearer {self.token}"},
            }

        ...

    ```

    And communication module can simply access to ``scicat_options.hearders`` instead of building the header itself.


## Argument Parser

As the argument parser is automatically built, there are some manual argument-configuration registries in ``scicat_configuration.py``.

### Helper Text Registry

[``scicat_configuration._HELP_TEXT``](https://github.com/SciCatProject/scicat-ingestor/blob/main/src/scicat_configuration.py)

<details>
    <summary>Why though?</summary>
    Unfortunately dataclass properties docstring cannot be parsed dynamically.<br>
    Therefore we made this registry to add useful help text to certain arguments.<br>
</details>

``_HELP_TEXT`` mapping proxy that holds all mappings from the long name to custom help-text.<br>
The keys should be the long name of the argument including its group without ``--``.

For example, if you want to add helper text to ``dry-run``, you have to add <br>
``"ingestion.dry-run": "Dry run mode. No data will be sent to SciCat."`` to the registry.

### Short Name Registry

[``scicat_configuration._SHORTENED_ARG_NAMES``](https://github.com/SciCatProject/scicat-ingestor/blob/main/src/scicat_configuration.py)

<details>
    <summary>Why though?</summary>
    Most of arguments will be passed to the ingestor from `config file`.<br>
    However `config-file` option can't be passed from the `config file` (obviously).<br>
    To make it more convenient to start the ingestor we wanted to give it a short name.
</details>

``_SHORTENED_ARG_NAMES`` mapping proxy that holds all mappings from the long name to short name.<br>
The keys should be the long name of the argument including its group without ``--``.

For example, if you want to use `d` for `dry run` configuration, you have to add <br>
``"ingestion.dry-run": "d"`` to the registry.
