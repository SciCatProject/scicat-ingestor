# Metada Schemas

`scicat-ingestor` parses or computes metadata from ingested dataset.<br>
`schema files` are a set of directives of how to collect/compute the metadata.<br>
The directives include `where(from which source - file, scicat or the schema itself, etc...)` to retrieve the information and `how` to compute/parse the information.
Using the directives, `scicat-ingestor` populate the dataset entry in `SciCat`.

In more detail, each schema file contains following information:
|   |   |
| - | - |
| schema selection | When to apply the schema to the file |
| variables definition | How to construct a `local variable set`, i.e. variable `pid` is the string from `/entry/entry_identifier_uuid` in the nexus file. |
| dataset creation | How to populate fields in a SciCat dataset using the `local variable set` |

The schema files are written in `json` format and all have `imsc.json` extension.<br>
`imsc` stands for SciCat `i`ngestor `m`etadata `sc`hema.<br>
Each schema file has mandatory fields.

## Content Structure
An `imsc` file has three main sections, *general*, *variables* and *schemas*.

| Section | Description |
| ------- | ----------- |
| *general* | A group of fields including `unique id`, `name of the schema`, `which instrument` it's associated with<br>and the matching-criteria that tells which file is relevant to this schema. <br> See [schema definition (general information)](#schema-definition-general-information) for more information. |
| *variables* | A set of `key-value` pairs retrieved from multiple sources.<br>They are later used in the schema section.<br>See [variable set instruction](#variable-set-instruction) for more information. |
| *schema* | A set of instructions how to populate `SciCat` dataset fields using `variables`.<br>See [SciCat dataset population instruction](#scicat-dataset-population-instruction) for more information. |

## Example

This is an example of schema file:

```json
{
  "order": "<NUMBER>",
  "id": "<UNIQUE_STRING>",
  "name" : "<NAME_OF_THE_SCHEMA>",
  "instrument" : "<COMMA_SEPARATED_LIST_OF_INSTRUMENTS_NAMES>",
  "selector" : "<OBJECT_WITH_SELECTION_RULES>",
  "variables" : {
    "<VARIABLE_NAME>" : "<VARIABLE_DEFINITION>"
  },
  "schemas" : {
    "<MNEMONIC_ENTRY_NAME>" : "<SCHEMA_ENTRY_DEFINITION>"
  }
}
```

## Schema Definition (General Information)

The first several fields of `imsc` files, that are not `variables` or `schemas`, contain meta data of the schema file for admins and data-curators to identify the schema. <br>
For ingestor, it contains the fields for ordering and matching schema files with the input data file to be ingested.

| Field Name | Field Type | Description |
| ---------- | ---------- | ----------- |
| id         | string     | Unique id of the schema. Subsequent version of the same schema should have different ids.<br> `uuid` is recommended, but there are not restrictions on the format. |
| name       | string     | Human readable name. *This field is only for admin to assign a meaningful name for the configuration.* |
| instrument | string      |  A list of relevant instrument names separated by comma(`,`). |
| order      | integer     | The order of the schemas. Ingestor sorts the schemas using the value of the `order`. The lower the order is, the earlier the schema is evaluated for applicability. In case a file matches multiple schema selector, the schema with lower order number will be used. |
| selector   | dict(object) \| string | A set of conditions for the schema to be matched with the input file. |

### Selector Example

Example 1:
The schema will be applied to files whose full path start with `/ess/data/coda` or `/ess/raw/coda`
```json
"selector" : {
    "or" : [
        "filename:starts_with:/ess/data/coda",
        "filename:starts_with:/ess/raw/coda"
    ]
}
```

Example 2:
The schema will be applied to files whose file name contains the word `coda`.
```json
"selector" : "filename:contains:coda"
```

Multiple selectors can be combined with `and` and `or` operators.<br>
Each selector can be a single string with three parts divided by colon(`:`), called `condensed selector`, or an object with the three equivalent fields, called `expanded selector`. <br>

The three components of each selector are:
- source of the value to use of the first operand
- operator
- second operand.

The syntax of the `condensed selector`:
```json
"source_op_1:operator:op_2"
```

The syntax of the `expanded selector`:
```json
{
"source" : "<SOURCE_OF_THE_FIRST_OPERAND>",
"operator" : "<OPERATOR>",
"operand_2" : "<OPERAND_2>"
}
```

Available Source Names:

| Source Name | Description |
| ----------- | ----------- |
| filename    | Path to the input file (provided as input argument with option *--nexus-file*) |
| datafile    | same as `filename` |
| nexusfile   | same as `filename` |

Available Operators:

| Operator Name | Description |
| ------------- | ----------- |
| starts_with   | the value retrieved from the source should start with  *operand_2* |
| contains      | the value retrieved from the source should contain *operand_2* |

Operand two is always interpreted as string.

## Variable Set Instruction

``variables`` section of the `imsc` file is a set of variable definitions.<br>
It can contain as many definitions as the user needs in order to populate the metadata.<br>
!!! note

    The order of the definitions in the section is important, as the ingestor will follow it.<br>
    Variables defined earlier can be used to define later ones.

### Example
``variables`` section should look like this:

```json
"variables" : {
  "<VARIABLE_NAME_1>" : "<VARIABLE_DEFINITION_1>",
  "<VARIABLE_NAME_2>" : "<VARIABLE_DEFINITION_2>",
}
```

``offline-ingestor`` will store these key-value pairs in memory and will use them for evaluating the `schema` section to populate `SciCat` dataset.

For example, given the following variables structure:
```json
"variables" : {
  "job_id": {
    "source": "VALUE",
    "value": "26e95e54",
    "value_type": "string"
  },
  "owner_group": {
    "source": "VALUE",
    "value": "group_<proposal_id>",
    "value_type": "string"
  },
  "access_groups": {
    "source": "VALUE",
    "value": ["group_1", "group_2"],
    "value_type": "string[]"
  }
}
```

The variables stored in memory will look like this:
```
"variables" : {
  "job_id": "26e95e54",
  "owner_group": "group_26e95e54",
  "access_groups": ["group_1", "group_2"],
}
```

### Variable definition

Each variable definition has the following structure:
```json
"<variable_name>" : {
    "source": "<source_of_value: NXS|SC|VALUE>",
    "value_type": "<type_of_value: string|dict|list|date|ingeter|string[]>",
    ...additional fields...
}
```

This definition will define a variable of name  *`variable_name`* of type *`type_of_value`* <br>
and the value of such variable will be retrieved from *`source_of_value`*. <br>
Additional fields might be required depending on the `source`.

#### Value Types
The field *value_type* accept the following options:

| Type Name | Dtype in `python` | Dtype in `typescript` | Description |
| --------- | ----------------- | --------------------- | ----------- |
| string    | str               | string                |             |
| string[]  | list[str]         | array[string]         |             |
| list      | list              | array                 |             |
| dict      | dict              | object                |             |
| integer   | int               | int                   |             |
| float     | float             | float                 |             |
| date      | str               | string                | ISO8601 format |
| email     | str               | string                |             |
| link      | str               | string                |             |

!!! note

    value types are not necessarily `dtype`.
    For example, `date` specifies not only the dtype but also the regex that it should be a datetime in `ISO8601` format.

Available `value-types` can be found in `_DtypeConvertingMap` in `scicat_dataset` module.

#### Sources
Currently, the source field only accepts the following options:

| Source Name | Description |
| ----------- | ----------- |
| NXS         | The nexus file specified as input argument with the option `--nexus-file`, i.e. the data file.|
| SC          | The api of the SciCat instance of reference which the ingestor is configured to interact with the configuration option *scicat.host*. |
| VALUE       | A static value specified directly in the definition |

Available `sources` can be found in `scicat_metadata` module. <br>
Here is more detailed description how to define variables from each `source`.

##### NXS (nexus)
A variable is retrieved from the nexus data file to be ingested. <br>

Syntax:
```json
"<variable_name>" : {
    "source": "NXS",
    "path": "<nexus_path>",
    "value_type": "<type_of_value>",
}
```

It means that the variable *`variable_name`* of type *`type_of_value`* will be retrieved from the *`nexus_path`* in the nexus file.

Example:
```
"job_id": {
    "source": "NXS",
    "path": "/entry/entry_identifier_uuid",
    "value_type": "string"
},
```
The variable *`job_id`* is parsed as a `string` from the nexus file at *`/entry/entry_identifier_uuid`*.

##### VALUE

A variable value is directly defined in the definition itself. <br>

Syntax:
```json
"<variable_name>" : {
    "source": "VALUE",
    ["operator": "<OPERATOR_NAME>",]
    "value": "<variable_value>",
    "value_type": "<type_of_value>",
}
```
It means thatthe variable *`variable_name`* of type *`type_of_value`* has the value, *`variable_value`*.
Additionally if the field `operator` is specified, the operator *`operator_name`* is applied to the value.<br>
The variable value can reference other variables that are previously defined i.e. `"<variable_name>"`. <br>
Any matching variables between `<>` will be resolved by the ingestor.

Example 1:
```
"pid": {
    "source": "VALUE",
    "value": "20.500.12269/23c40f04-de76-11ef-897f-bb426f5f764f",
    "value_type": "string"
},
```
A variable named *`pid`* has the value of *`20.500.12269/23c40f04-de76-11ef-897f-bb426f5f764f`* of dtype *`string`*.

Example 2:
```
"pid": {
    "source": "VALUE",
    "value": "20.500.12269/<job_id>",
    "value_type": "string"
},
```
If there is variable *`job_id`* is with a *`string`* value, *`cc59b538-de76-11ef-b192-d3d4305b1b90`*, <br>
the variable *`pid`* will have the value of *`20.500.12269/cc59b538-de76-11ef-b192-d3d4305b1b90`* of type *`string`*. <br>


###### Value Operators

A value definition can contain an additional field *`operator`* which specifies an operator to be applied to the value.<br>
Depending on the `operator`, `variable definition` may need extra fields.<br>
The current list of the operators available with their functionality is the following:

| Operator Name | Functionality | Mandatory Extra Fields |
| ------------- | ------------- | ---------------------- |
| getitem       | Assuming the value is a `list` or a `dict`, select an element by the key, `field`. | `field` |
| str_replace   | Replace a pattern with a string within the value. | `pattern`, `replacement` |
| to_upper      | Convert the value to upper case | |
| to_lower      | Convert the value to lower case | |
| urlsafe       | Convert the value to a safe format to be used in a url | |
| join_with_space | Assuming the value is an array, joins all the elements of the array with a space as separating character as a single string | |
| dirname       | Assuming the value is a file system path, returns the parent folder. <br>It effectively eliminates the last element of the path. | |
| dirname-2     | Assuming the value is a file system path, returns the grand-parent folder <br>It effectively eliminates the last two elements of the path. | |
| filename      | Assuming the value is a file system path, returns the file name of the path. | |
| DO_NOTHING    | Returns the value itself.<br>It is useful when you want to use the same value for multiple variables. |

Here are examples of some complex operators that have extra fields:

- getitem

  If there is an existing variable `proposal_data` like this:
  ```python
  local_variables = {
    "proposal_data": {
        "pi_firstname" : "John",
        "pi_lastname" : "Doe"
    }
  }
  ```

  The following variable definition will resolve to a variable *`pi_firstname`* with value `"John"`.

  ```json
  "pi_firstname": {
    "source": "VALUE",
    "operator": "getitem",
    "value": "<proposal_data>",
    "field" : "pi_firstname",
    "value_type": "string"
  }
  ```

- str_replace

  If there is an existing variable `raw_folder` like this:
  ```python
  local_variables = {"raw_folder": "/ess/raw/instrument/year/proposal"}
  ```

  The following variable definition will resolve to a variable *`source_folder`* with value `"/ess/data/instrument/year/proposal"`.

  ```json
  "source_folder": {
    "source": "VALUE",
    "operator": "str-replace",
    "value": "<raw_folder>",
    "pattern": "ess/raw",
    "replacement": "ess/data",
    "value_type": "string"
  },
  ```

##### SC (SciCat)

A variable is retrieved from a SciCat instance. <br>

Syntax:
```
"<VARIABLE_NAME>" : {
    "source": "SC",
    "url": "<ENDPOINT_URL>",
    "field" : "<NAME_OF_THE_FIELD>
    "value_type": "<TYPE_OF_VALUE>",
}
```
It means a variable of  *`variable_name`* of type *`type_of_value`* will have the value
retrieved from the `SciCat` instance, with the endpoint specified by the field `url`.<br>
If `field` is specified, it will extract the exact field from the results, otherwise it will assign the full object returned by scicat.<br>
The url is relative to the `SciCat` instance endpoint in the [`configuration`](./configuration.md#scicat-scicat).

Example:
```
"proposal_data": {
  "source": "SC",
  "url": "proposals/<proposal_id>",
  "field" : "",
  "value_type": "dict"
},
```

The ingestor will put through a `GET` request to the endpoint `proposals` with *`proposal_id`*. <br>
*`proposal_id`* should be defined already.<br>
`SciCat` should return a `dict` object containing all the information regarding the `proposal`.<br>
The variable *proposal_data* will have the returned object as its value.

## Scicat Dataset Population Instruction

Under the `schema` section of the imsc file, `ingestor` finds how to populate the SciCat dataset fields using [`variables`](#variable-set-instruction).<br>
`schema` section can contain arbitrary number of dataset definitions.<br>
*The order of scicat dataset definitions is not important.*

Syntax:
```json
"schema" : {
  "<ASSIGNMENT_NAME_1>" : "<FIELD_ASSIGNMENT_1>",
  "<ASSIGNMENT_NAME_2>" : "<FIELD_ASSIGNMENT_2>",
}
```

> The names for the dataset definition, i.e. `"<ASSIGNMENT_NAME>"`, are not used by the ingestor. <br>
They are meant to be human readable for the data curator.

Example:
```json
"schema" : {
  "pid": {
    "field_type": "high_level",
    "machine_name": "pid",
    "value": "<pid>",
    "type": "string"
  },
  "type" : {
    "field_type": "high_level",
    "machine_name": "type",
    "value": "raw",
    "type": "string"
  },
  "proposal_id": {
    "field_type": "high_level",
    "machine_name": "proposalId",
    "value": "<proposal_id>",
    "type": "string"
  },
  "dataset_name": {
    "field_type": "high_level",
    "machine_name": "datasetName",
    "value": "<dataset_name>",
    "type": "string"
  }
}
```

The dataset `pid` will use the variable *`pid`* as a value, <br>
the dataset `type` will have a value of `"raw"`, <br>
the dataset `proposalId` will use the variable *`proposal_id`* as a value, <br>
and the dataset `datasetName` will use the variable *`dataset_name`* as a value.

!!! note

    The `variable` name should be wrapped in `<>` so that `ingestor` recognizes it as a variable.


### Field assignment

There are two types of scicat dataset definition:

- high level field
- scientific metadata field

Each definition type is described in the following sub sections:

#### High Level Field

Syntax:
```json
"<ASSIGNMENT_NAME>": {
  "field_type": "high_level",
  "machine_name": "<SCICAT_DATASET_FIELD>",
  "value": "<FIELD_VALUE>",
  "type": "<FIELD_TYPE>"
}
```

Dataset definition `<ASSIGNMENT_NAME>` will have value `<FIELD_VALUE>` of type `<FIELD_TYPE>`. <br>
`<SCICAT_DATASET_FIELD>` is the field name in the SciCat dto(data transfer object). <br>
Invalid field name will cause an error and the dataset entry will not be created by the `SciCat` backend.

Example with static value:
```json
"type" : {
  "field_type": "high_level",
  "machine_name": "type",
  "value": "raw",
  "type": "string"
},
```
This assignment will assign the value `"raw"` to the dataset field `type`.

Example using variable:
```json
"creation_location": {
  "field_type": "high_level",
  "machine_name": "creationLocation",
  "value": "ESS:CODA:<instrument_name>",
  "type": "string"
},
```

Assuming that variable *`instrument_name`* has the value `dream`, <br>
`createLocation` field in the dto will have the value, `"ESS:CODE:dream"`.

#### Scientific Metadata

Syntax:
```json
"<ASSIGNMENT_NAME>": {
  "field_type": "scientific_metadata",
  "machine_name": "<SCIENTIFIC_METADATA_MACHINE_FIELD_NAME>",
  "human_name": "<SCIENTIFIC_METADATA_HUMAN_FIELD_NAME>",
  "value": "<FIELD_VALUE>",
  "type": "<VALUE_TYPE>"
}
```

Ingestor will create a scientific metadata entry *`assignment_name`*, <br>
named `<SCIENTIFIC_METADATA_MACHINE_FIELD_NAME>` (which is machine consumable) <br>
with a value of `<FIELD_VALUE>` of type `<VALUE_TYPE>`. <br>
It will also add the optional field `human_name` with the value `<SCIENTIFIC_METADATA_HUMAN_FIELD_NAME>`. which will appear in the `SciCat` frontend.

Example 1:
```json
"start_time": {
  "field_type": "scientific_metadata",
  "machine_name": "start_time",
  "human_name": "Start Time",
  "value": "<start_time>",
  "type": "date"
},
"end_time": {
  "field_type": "scientific_metadata",
  "machine_name": "end_time",
  "human_name": "End Time",
  "value": "<end_time>",
  "type": "date"
},
```

Assuming that variable *`start_time`* has value `"2025-01-20 12:00:00"` <br>
and variable *`end_time`* has value `"2025-01-20 12:30:00"`, <br>
the scientific metadata object will look like this:

```python
"scientific_metadata" {
  "start_time" : {
    "value" : "2025-01-20 12:00:00"
    "unit" : ""
    "human_name" : "Start time"
    "type" : "date"
  },
  "end_time" : {
    "value" : "2025-01-20 12:30:00"
    "unit" : ""
    "human_name" : "End time"
    "type" : "date"
  }
}
```
