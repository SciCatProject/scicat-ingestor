# Online Ingestor

The ``online-ingestor`` is an **asynchronous active daemon program**.
It means ``online-ingestor`` pulls notification/information from a message broker and process them as it finds, instead of synchronously triggered by the file being written.

!!! note

    `scicat-ingestor` does not come with daemon installation helper.
    You will have to set it up by your self.

    <!-- TODO: Add systemd setting tutorial -->

!!! warning

    It is only tested on Ubuntu (>=22.04) as we do not have any plans to support other types of OS.


<details>
<summary>Why is it not done by file-writer instead?</summary>

File-writer should be a very robust program as it is very critical for collecting data so it is better to have a single concern.
As we keep file writing and file ingestion decoupled, even if either file-writer or scicat-ingestor fails, the other program can continue doing their job.
Also they are maintained by different teams so for maintenance it is easier to keep the interface asynchronous rather than building a monolithic program.

</details>

Whenever it can pull a notification about a new dataset, it spawns a background process where the ``offline-ingestor`` ingests the file.

``online-ingestor`` spawns only a limited number of `offline-ingestor` processes.
Whenever the number of `offline-ingestor` processes reaches the limitation,
it stops and wait until certain background processes are done.
The number of processes limitation is configurable by ``max_offline_ingestors`` and ``offline_ingestor_wait_time``.

!!! note
    The ``scicat-ingestor`` is developed for ESS specifically therefore it only has support to ``kafka`` broker and expects specific flatbuffer schema type(wfdn) used by our filewriter.
    Generalization and adoption of different delivery and messaging system will be considered on a per-request base.
    If you are interested in using ingestor with other frameworks, please contact us on our [issue board](https://github.com/SciCatProject/scicat-ingestor/issues) or directly to the maintainers.

## How to Run

As ``online-ingestor`` is the main purpose of this project, it has an entry-point of script as ``scicat_ingestor``.
> See [getting started page](../getting-started.md)

Or you can also run it as a module or as a script itself.

For example:
``` bash
<path_to_the_selected_python_executable> \
<full_path_to_the_ingestor_executable_folder>/scicat_online_ingestor.py \
-c <full_path_to_the_configuration_file>

```

In the case of the ESS test environment, the command looks like this:
``` bash
/root/micromamba/envs/scicat-ingestor/bin/python \
/ess/services/scicat-ingestor/software/src/scicat_online_ingestor.py \
-c /ess/services/scicat-ingestor/config/scicat_ingestor_config.yml
```

## Configuration

> See [configuration page](./configuration.md) for more details.

Online ingestor uses only the following sections of the configurations:
- ingestion
- kafka
- logging

The rest is simply passed to the offline ingestor from the file.

> See [ADR-000#configuration](../developer-guide/adrs.md#configuration) for why `online-ingesetor` and `offline-ingestor` share the same configuration file.
