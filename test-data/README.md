Files here are small mock-files for testing.

Each file should have a code-snippet that was used to create if applicable.

# Nexus Files
## small_coda.hdf

```python
# In a notebook cell.
!cp 443503_00031010.hdf copied_coda.hdf
import h5py

with h5py.File('copied_coda.hdf', 'r+') as f:
    instrument_gr = f['entry/instrument']
    keeping_names = ['name', '_publication']
    for name in instrument_gr:
        if name not in keeping_names:
            del instrument_gr[name]

    # Copy the rest of the file
    with h5py.File('small-coda.hdf', 'w') as new_f:
        # copy everything
        f.copy('entry', new_f)

```

## small_ymir.hdf

```python
# In a notebook cell.
!cp 876380_00011465.hdf copied_ymir.hdf
import h5py

with h5py.File('copied_ymir.hdf', 'r+') as f:
    instrument_gr = f['entry/instrument']
    keeping_names = ['name', '_publication']
    for name in instrument_gr:
        if name not in keeping_names:
            del instrument_gr[name]

    # Copy the rest of the file
    with h5py.File('small-ymir.hdf', 'w') as new_f:
        # copy everything
        f.copy('entry', new_f)

```
