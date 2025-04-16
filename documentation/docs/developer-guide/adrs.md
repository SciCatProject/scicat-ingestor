# Architecture Decision Records

Here we keep records of important software architecture decisions and the reasonabouts.

## ADR-000: Decouple continuous discovery process and individual dataset ingestion process.

`scicat ingestor` has two main responsibilities:
    - Continuous discovery of a new dataset with related files
    - Individual dataset ingestion from the discovery

Previously (<25.01.0) ``scicat ingestor`` was single process program that continuously processes messages and files in a loop.
In other words, both responsibilities were deelpy coupled.

As the ``scicat ingestor`` under went project wide refactoring, <br>
we decided to decouple those responsibilities(functionalities) and extract individual dataset ingestion as an independent tool.

### Advantages

Here are some of advantages we discovered as we decoupled the discovery process and ingestion process.

#### Smaller Tests

A single program, as it was initially, was hard to test and also to maintain.
For example, we had to send ``kafka`` message to trigger the ingestion and make the ingestor parse ``metadata`` from files just to test if it can ingest file to ``scicat`` accordingly.
If they are decoupled we can split this huge test into three smaller tests:
    - ``kafka`` message processing
    - ``metadata`` extraction
    - ``scicat`` ingestion
This decoupling helps to implement faster unittests/integration tests on a smaller scope.

#### Smaller Usage

As the dataset ingestion is now an completely independently tool from the discovery process, we can easily run ingestion multiple times in case of error.

#### Multi Processes with Central Control

Discovery process(online ingestor) spawns a sub process to start the ingestion process and continue listening to the next dataset.
In reality, ``online ingestor`` spawns multiple processes to start ``offline ingestor`` as it could take a few seconds or even a few minutes depending on the metadata it needs to compute.
Even if one of processes fails due to faulty metadata or unexpected structure of dataset, it will not affect the rest of healthy files and ingestions as it is on a separate process.

#### Less Configurations

As the ingestion process(``offline ingestor``) now do not communicate with kafka anymore,
it can use subset of ``online ingestor`` configurations,
which makes it easier to go through the list of the configurations dedicated to ``offline ingestor``.

#### Easier Maintenance

Due to all advantages we mentioned above, the maintenance cost considerably reduced.
It takes less for testing, hence less time to release the software.

### Visualization of Architecture

!!! note

    These diagrams might be updated and be different from the first design.

#### Ingestor Flow Chart - Bird Eye View

<!-- Mermaid chart does not support different shapes for subgraph-->
<!-- And we wanted make the offline ingestor as `processes` shape -->
<!-- As there will be multiple processes of them. -->
<!-- So we made svg image of the diagram instead using mermaid. -->
<!-- It very likely that they will support different shapes for subgraph in the future though...! -->
![image](../_mermaid_charts/_ingestor_flow_birdeyeview.svg)

#### Ingestor Flow Chart - Detail
{%
    include-markdown "../_mermaid_charts/_ingestor_flow_details.md"
%}

## ADR-001: Use ``dataclass`` instead of ``jinja`` or ``dict`` to create dataset/data-block instances.
We need a dict-like template to create dataset/data-block instances via scicat APIs.
### Reason for not using ``dict``
It used to be implemented with ``dict`` but it didn't have any verifying layer so anyone could easily break the instances without noticing or causing errors in the upstream layers.
### Reason for not using ``jinja``

``Jinja`` template could handle a bit more complicated logic within the template, i.e. ``for`` loop or ``if`` statement could be applied to the variables.
However, the dataset/data-block instances are not complicated to utilize these features of ``jinja``.

### Reason for using ``dataclasses.dataclass``
First we did try using ``jinja`` but the dataset/data-block instances are simple enough so we replaced ``jinja`` template with ``dataclass``.
``dataclass`` can verify name and type (if we use static checks) of each field.
It can be easily turned into a nested dictionary using ``dataclasses.asdict`` function.

### Downside of using ``dataclass`` instead of ``jinja``
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

!!! warning
    <h1> No Multi-type Arguments </h1>
    We decided **NOT** to support **multiple types of arguments/configuration option** due to this ADR.
    It is not impossible to support it, but the advantange is not worth the effort of handling multiple types.
    Especially it makes the auto generator of json configuration file much more difficult than how it is now.

    For example, there can be multiple kafka brokers so in principle we could allow a list of string as an argument type or a single string value.
    However we decided to keep it only as a string, and if multiple brokers are needed, user should write them joined by comma(,).
    On the other hand, access group option is always `list` even if there may be only one access group.
