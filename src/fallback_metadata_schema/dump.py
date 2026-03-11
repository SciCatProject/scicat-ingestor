"""Fallback metadata schema storing helper."""

import pathlib

from scicat_metadata import (
    MetadataItemConfig,
    MetadataSchema,
    VariableConfigNexusFile,
    VariableConfigScicat,
    VariableConfigValue,
)

_FallBackSchema = MetadataSchema(
    id='scicat-ingestor-fallback-schema',
    name='Fall Back Metadata Schema',
    instrument='*',
    selector='*',
    order=9999999999,  # No reason. Just a big number that represents the low preference to this schema.
    variables={
        'job_id': VariableConfigNexusFile(
            source='NXS',
            value_type='string',
            path='/entry/entry_identifier_uuid',
        ),
        'pid': VariableConfigValue(
            source='VALUE', value_type='string', value='20.500.12269/<job_id>'
        ),
        'proposal_id': VariableConfigNexusFile(
            source='NXS',
            value_type='string',
            path='/entry/experiment_identifier',
        ),
        'pi_firstname': VariableConfigScicat(
            source='SC',
            url='proposals/<proposal_id>',
            field='pi_firstname',
            value_type='string',
        ),
        'pi_lastname': VariableConfigScicat(
            source='SC',
            url='proposals/<proposal_id>',
            field='pi_lastname',
            value_type='string',
        ),
        'pi_email': VariableConfigScicat(
            source='SC',
            url='proposals/<proposal_id>',
            field='pi_email',
            value_type='string',
        ),
        'dataset_name': VariableConfigNexusFile(
            source='NXS',
            path='entry/title',
            value_type='string',
        ),
        'source_folder': VariableConfigValue(
            source='VALUE',
            operator='dirname-2',
            value='<data_file_path>',
            value_type='string',
        ),
    },
    schema={
        'pid': MetadataItemConfig(
            machine_name='pid', field_type='high_level', value='<pid>', type='string'
        ),
        'dataset_name': MetadataItemConfig(
            machine_name='datasetName',
            field_type='high_level',
            value='<dataset_name>',
            type='string',
        ),
        'principal_investigator': MetadataItemConfig(
            machine_name='principalInvestigator',
            field_type='high_level',
            value='<pi_firstname> <pi_lastname>',
            type='string',
        ),
        'creation_location': MetadataItemConfig(
            machine_name='creationLocation',
            field_type='high_level',
            value='ESS',
            type='string',
        ),
        'owner': MetadataItemConfig(
            machine_name='owner',
            field_type='high_level',
            value='<pi_firstname> <pi_lastname>',
            type='string',
        ),
        'owner_email': MetadataItemConfig(
            machine_name='ownerEmail',
            field_type='high_level',
            value='<pi_email>',
            type='string',
        ),
        'source_folder': MetadataItemConfig(
            machine_name='sourceFolder',
            field_type='high_level',
            value='<source_folder>',
            type='string',
        ),
        'contact_email': MetadataItemConfig(
            machine_name='contactEmail',
            field_type='high_level',
            value='<pi_email>',
            type='string',
        ),
        'creation_time': MetadataItemConfig(
            machine_name='creationTime',
            field_type='high_level',
            value='<now>',
            type='date',
        ),
    },
)


if __name__ == "__main__":
    target_file_path = pathlib.Path(__file__).parent / 'fallback.imsc.yml'
    _FallBackSchema.save_file(target_file_path, exist_ok=True)
    original_lines = target_file_path.read_text()
    target_file_path.write_text(
        '# Written by: ' + __spec__.name + '\n' + original_lines
    )
