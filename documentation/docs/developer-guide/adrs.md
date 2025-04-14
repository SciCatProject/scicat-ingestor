# Architecture Decision Records

Here we keep records of important software architecture decisions and the reasonabouts.

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
