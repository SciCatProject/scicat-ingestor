# SciCat Jobs

A metadata schema can define one or more SciCat jobs to create after the dataset and its origdatablock have been created. Jobs are useful for handing post-processing work to another service, such as embargo handling, validation, or analysis.

Job definitions are configured in the schema file, while the SciCat jobs API endpoint is configured in the ingestor configuration file.

## Configuration location

The `jobs` section is a top-level section of an `.imsc.yml` file. It is a sibling of `variables` and `schema`; it must not be placed inside the dataset `schema` section.

```yaml
variables:
  # Variable definitions ...

schema:
  # Dataset field definitions ...

jobs:
  # Job definitions ...
```

## Job definition

Each entry under `jobs` has a local name and the following required fields:

| Field | Description |
| --- | --- |
| `type` | SciCat job type. This must match a job type configured and supported by the SciCat deployment. |
| `job_params` | Parameters sent to SciCat as `jobParams`. This must be a dictionary. |
| `owner_user` | SciCat job owner user. |
| `owner_group` | SciCat job owner group. |
| `contact_email` | Contact email associated with the job. |

The name assigned to the job is a human-readable configuration key. It is used for logging and result reporting, but is not sent as a field in the job payload.

For example:

```yaml
jobs:
  embargo_period:
    type: embargo_period
    job_params:
      datasetList:
        - pid: <pid>
          files: []
    owner_user: pit_manager
    owner_group: pit_management
    contact_email: <pi_email>
```

The corresponding payload sent to SciCat is equivalent to:

```json
{
  "type": "embargo_period",
  "jobParams": {
    "datasetList": [
      {
        "pid": "20.500.12269/example-dataset-pid",
        "files": []
      }
    ]
  },
  "ownerUser": "pit_manager",
  "ownerGroup": "pit_management",
  "contactEmail": "pi@example.org"
}
```

The ingestor converts the YAML names `job_params`, `owner_user`, `owner_group`, and `contact_email` to SciCat's API names `jobParams`, `ownerUser`, `ownerGroup`, and `contactEmail`.

## Referencing metadata variables

Job values use the same variable syntax as dataset definitions: enclose a variable name in angle brackets.

```yaml
variables:
  proposal_id:
    source: NXS
    path: /entry/experiment_identifier
    value_type: string
  pi_email:
    source: SC
    url: proposals/<proposal_id>
    field: pi_email
    value_type: string

jobs:
  validation:
    type: dataset_validation
    job_params:
      datasetPid: <pid>
      proposalId: <proposal_id>
      source: "ingestor-<proposal_id>"
    owner_user: validation_service
    owner_group: data_quality
    contact_email: <pi_email>
```

Variables can be used in:

- `job_params`
- `owner_user`
- `owner_group`
- `contact_email`
- strings containing more than one part, such as `"dataset-<proposal_id>"`
- lists and dictionaries contained in `job_params`

An unresolved variable causes job payload rendering to fail. Every variable referenced by a job should therefore be defined in the schema, except for `pid` as described below.

### Dataset PID

The ingestor updates the `pid` variable with the PID of the dataset returned by SciCat immediately before creating jobs. Consequently, `<pid>` is the recommended way to associate a job with the newly created dataset:

```yaml
jobs:
  processing:
    type: processing
    job_params:
      datasetPid: <pid>
    owner_user: processing_service
    owner_group: processing
    contact_email: processing@example.org
```

## Multiple jobs

A schema can create multiple jobs. Each named entry produces one POST request to the SciCat jobs endpoint.

```yaml
jobs:
  validation:
    type: dataset_validation
    job_params:
      datasetPid: <pid>
      strict: true
    owner_user: validation_service
    owner_group: data_quality
    contact_email: quality@example.org

  processing:
    type: processing
    job_params:
      datasets:
        - pid: <pid>
          files: []
      priority: high
    owner_user: processing_service
    owner_group: processing
    contact_email: processing@example.org
```

The order of job definitions should not be used to express dependencies. Jobs are posted from the configured mapping, and the job configuration does not provide dependency or retry semantics.

## `job_params` structure

`job_params` must be a dictionary. Values may be scalars, lists, or dictionaries inside lists. The existing embargo example uses a list of objects:

```yaml
job_params:
  datasetList:
    - pid: <pid>
      files: []
```

Keep the top-level `job_params` dictionary flat. A directly nested dictionary is rejected by the current schema parser:

```yaml
# Invalid: directly nested dictionaries are not supported here
job_params:
  dataset:
    pid: <pid>
```

Use the parameter structure expected by the configured SciCat job type. The ingestor does not validate job-specific parameter names; SciCat performs that validation when it receives the request.

For predictable payload types, prefer values extracted through variables or string values. The current renderer is primarily designed for strings, variable values, lists, and dictionaries; static non-string scalar values may be rendered as strings.

## SciCat endpoint configuration

The schema job section requires a jobs endpoint in the main ingestor configuration:

```yaml
scicat:
  host: https://scicat.example.org
  api_endpoints:
    jobs: v4/jobs
```

The endpoint is joined to `scicat.host`. The sample configuration uses `v4/jobs`:

```yaml
scicat:
  api_endpoints:
    proposals: v3/proposals
    origdatablocks: v4/origdatablocks
    instruments: v3/instruments
    samples: v4/samples
    jobs: v4/jobs
    health: v3/health
```

The configured SciCat instance must have the corresponding job type and job-processing configuration. The ingestor only constructs and submits the job payload.

## Processing lifecycle and failures

For a normal ingestion, the offline ingestor:

1. Creates the SciCat dataset.
2. Creates the origdatablock.
3. Posts each configured job to the jobs endpoint.
4. Logs a summary of successful and failed job requests.

A job request is not sent when dataset or origdatablock creation fails. Job failures are reported separately from dataset creation, so a failed job does not undo the already-created dataset or origdatablock.

When `ingestion.dry_run` is enabled, no dataset or job is posted. The ingestor only renders and logs the configured job payloads.

## Complete example

```yaml
order: 100
id: 01234567-89ab-cdef-0123-456789abcdef
name: Example metadata schema
instrument: example
selector: filename:starts_with:/data/example

variables:
  pid:
    source: NXS
    path: /entry/entry_identifier_uuid
    value_type: string
  proposal_id:
    source: NXS
    path: /entry/experiment_identifier
    value_type: string
  pi_email:
    source: SC
    url: proposals/<proposal_id>
    field: pi_email
    value_type: string

schema:
  dataset_name:
    field_type: high_level
    machine_name: datasetName
    value: example-<proposal_id>
    type: string

jobs:
  embargo_period:
    type: embargo_period
    job_params:
      datasetList:
        - pid: <pid>
          files: []
    owner_user: pit_manager
    owner_group: pit_management
    contact_email: <pi_email>

  validation:
    type: dataset_validation
    job_params:
      datasetPid: <pid>
      proposalId: <proposal_id>
    owner_user: validation_service
    owner_group: data_quality
    contact_email: quality@example.org
```
