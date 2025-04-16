# Overview

As mentioned in the [ADR-001](../developer-guide/adrs.md#adr-000-decouple-continuous-discovery-process-and-individual-dataset-ingestion-process), `scicat-ingestor` has two main components, ``online-ingestor`` and ``offline-ingestor``.

``online-ingestor`` is a daemonic program that runs ``offline-ingestor`` in real-time.
In other words, ``online-ingestor`` is higher level program than ``offline-ingestor``.

This page has various diagrams that shows how the ingestion flow or sequence is done.
You can find the relationship between ``online`` and ``offline`` ingestor from the diagram.

See [online-ingestor page](./online-ingestor.md) and [offline-ingestor page](./offline-ingestor.md) for more details.

## Infrastructure around Scicat Ingestor

``scicat-ingestor`` is written for specific infrastructure setup like below:

{%
    include-markdown "../_mermaid_charts/_infra_structure.md"
%}

## Ingestor Flow Chart - Bird Eye View

<!-- Mermaid chart does not support different shapes for subgraph-->
<!-- And we wanted make the offline ingestor as `processes` shape -->
<!-- As there will be multiple processes of them. -->
<!-- So we made svg image of the diagram instead using mermaid. -->
<!-- It very likely that they will support different shapes for subgraph in the future though...! -->
![image](../_mermaid_charts/_ingestor_flow_birdeyeview.svg)

## Ingestor Flow Chart - Detail
{%
    include-markdown "../_mermaid_charts/_ingestor_flow_details.md"
%}

## Ingestor Sequence Chart
{%
    include-markdown "../_mermaid_charts/_file_ingestion_sequence.md"
%}
