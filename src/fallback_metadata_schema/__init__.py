import pathlib

from scicat_metadata import MetadataSchema

_cur_dir = pathlib.Path(__file__).parent
_fallback_schemafile_names = list(_cur_dir.glob(pattern='*.imsc.yml'))

FALLBACK_SCHEMA_REGISTRY = {
    file_name.name.removesuffix('.imsc.yml'): _cur_dir / file_name
    for file_name in _fallback_schemafile_names
}


def get_fallback_schema(name_or_path: str) -> MetadataSchema | None:
    if name_or_path == '':
        return None

    schema_path = pathlib.Path(name_or_path)
    if schema_path.exists():
        return MetadataSchema.from_file(schema_path)

    if name_or_path in FALLBACK_SCHEMA_REGISTRY:
        return FALLBACK_SCHEMA_REGISTRY[name_or_path]

    raise ValueError(
        f"No fallback schema found with/in name/path: {name_or_path}. "
        f"Available names are: ({', '.join(FALLBACK_SCHEMA_REGISTRY.keys())}). "
        "If fallback schema should not be used, remove it from the configuration."
    )
