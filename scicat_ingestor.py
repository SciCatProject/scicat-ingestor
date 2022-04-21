#!/usr/bin/env python3
#
# 

from datetime import datetime
import json
import sys
import uuid
import re
import os
import argparse
import logging

from user_office import UserOffice
#from scicat import SciCat

from kafka import KafkaConsumer
from streaming_data_types import deserialise_wrdn


sys.path.insert(
    0, 
    os.path.abspath('../pyscicat')
)
import pyscicat.client as pyScClient
import pyscicat.model as pyScModel



def main(config, logger):

    # instantiate kafka consumer
    kafka_config = config["kafka"]
    logger.info('Connecting to Kafka server {} on topic {}'.format(
        kafka_config["bootstrap_servers"],
        kafka_config["topic"]
    ))
    consumer = KafkaConsumer(
        kafka_config["topic"],
        group_id=kafka_config["group_id"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        auto_offset_reset=kafka_config["auto_offset_reset"],
    )

    # instantiate connector to user office
    # retrieve relevant configuration
    user_office_config = config["user_office"]
    logger.info('Connecting to User Office running on {} with username {}'.format(
        user_office_config["host"],
        user_office_config["username"]
    ))
    user_office = UserOffice(user_office_config["host"])
    user_office.login(user_office_config["username"],user_office_config["password"])

    # instantiate connector to scicat
    # retrieve relevant configuration
    scicat_config = config["scicat"]
    # instantiate a pySciCat client
    logger.info('Instantiating SciCat client')
    logger.info('SciCat instance : {}'.format(scicat_config['host']))
    logger.info('Login as user : {}'.format(scicat_config['username']))
    scClient = pyScClient.ScicatClient(
        base_url=scicat_config['host'],
        username=scicat_config["username"],
        password=scicat_config["password"],
    )

    # main loop, waiting for messages
    for message in consumer:
        try:
            data_type = message.value[4:8]
            if data_type == b"wrdn":
                logger.info("Received writing done message from file writer")
                entry = deserialise_wrdn(message.value)
                if entry.error_encountered:
                    logger.error("Unable to de-serialize message")
                    continue

                logger.info(entry)
                if entry.metadata is not None:
                    metadata = json.loads(entry.metadata)
                    print(metadata)
                    # retrieve proposal id, if present
                    proposal_id = None
                    if "proposal_id" in metadata:
                        proposal_id = get_proposal_id(
                            metadata["hdf_structure"],
                            config['dataset']['default_proposal_id']
                        )
                    if proposal_id is None:
                        # assign default proposal id
                        proposal_id = config['dataset']['default_proposal_id']
                    
                    # We assume that all the relevant information are already in scicat
                    proposal = scClient.get_proposal_by_pid(proposal_id)

                    # create an owneable object to be used with all the other models
                    # all the fields are retrieved directly from the simulation information
                    logger.info('Instantiate ownable model')
                    ownable = pyScModel.Ownable(
                        ownerGroup=(
                            config['dataset']['ownable']['ownerGroup'] 
                            if 'ownerGroup' in config['dataset']['ownable'].keys() 
                            else proposal_id
                        ), 
                        accessGroups=config['scicat']["ownable"]['accessGroups']
                    )

                    # load instrument by id or by name
                    instrument_id = None
                    if 'instrument_id' in config['dataset'].keys() and config['dataset']['instrument_id']:
                        instrument_id = config['dataset']['instrument_id']
                    elif "instrument_id" in metadata and metadata["instrument_id"]:
                        instrument_id = metadata['instrument_id']
                    instrument = scClient.get_instrument_by_pid(instrument_id) if instrument_id else {}

                    if not instrument and "instrument_name" in metadata and metadata["instrument_name"]:
                        instrument = scClient.get_instrument_by_name(metadata["instrument_name"])
                        

                    # find sample information
                    sample_id = None
                    if 'simple_id' in config['dataset'].keys() and config['dataset']['sample_id']:
                        sample_id = config['dataset']['sample_id']
                    elif "sample_id" in metadata and metadata["sample_id"]:
                        sample_id = metadata['sample_id']
                    sample = scClient.get_sample_by_pid(sample_id) if sample_id else {}


                    # create dataset object from the pyscicat model
                    # includes ownable from previous step
                    logger.info('Instantiating dataset model')
                    dataset = create_dataset(
                        metadata, 
                        proposal, 
                        instrument,
                        sample,
                        ownable
                    )

                    logger.info('Creating dataset on SciCat')
                    created_dataset = scClient.upload_new_dataset(dataset)
                    logger.info('Dataset created with pid {}'.format(created_dataset['pid']))


                    # create origdatablock object from pyscicat model
                    logger.info('Instantiating original datablock')
                    origDatablock = create_orig_datablock(
                        created_dataset["pid"], 
                        entry.file_size if "file_size" in entry else 0,
                        entry.file_name,
                        ownable
                    )
                    # create origDatablock associated with dataset in SciCat
                    # it returns the full object including SciCat id assigned when created
                    logger.info('Creating original datablock in SciCat')
                    created_orig_datablock = scClient.upload_dataset_origdatablock(origDatablock)
                    logger.info('Original datablock created with internal id {}'.format(created_orig_datablock['_id']))

                else:
                    logger.warning("No metadata in this message")
        except KeyboardInterrupt:
            logger.info("Exiting ingestor")
            sys.exit()
        except Exception as error:
            logger.warning("Error ingesting the message: {}".format(error))



def get_config(config_file: str = 'config.json') -> dict:
    with open(config_file, "r") as config_file:
        data = config_file.read()
        return json.loads(data)



def create_dataset(
    metadata: dict, 
    proposal: dict, 
    instrument: dict, 
    sample: dict,
    ownable: pyScModel.Ownable
) -> dict:
    # prepare info for datasets
    dataset_pid = str(uuid.uuid4())
    dataset_name = metadata["run_name"] \
        if "run_name" in metadata \
        else "Dataset {} for proposal {}".format(dataset_pid,proposal.get('pid','unknown'))
    dataset_description = metadata["run_description"] \
        if "run_description" in metadata \
        else "Dataset: {}. Proposal: {}. Sample: {}. Instrument: {}".format(
            dataset_pid,
            proposal.get('proposalId','unknown'),
            instrument.get('pid','unknown'),
            sample.get('sampleId','unknown'))
    principal_investigator = proposal["pi_firstname"] + " " + proposal["pi_lastname"]
    email = proposal["pi_email"]
    instrument_name = instrument.get("name","")
    source_folder = (
        "/nfs/groups/beamlines/" + instrument_name if instrument_name else "unknown" + "/" + proposal["proposalId"]
    ) 
    
    # create dictionary with all requested info
    return pyScModel.RawDataset(
        **{
            "pid" : dataset_pid,
            "datasetName": dataset_name,
            "description": dataset_description,
            "principalInvestigator": principal_investigator,
            "creationLocation": instrument.get("name",""),
            "scientificMetadata": prepare_metadata(flatten_metadata(metadata)),
            "owner": principal_investigator,
            "ownerEmail": email,
            "contactEmail": email,
            "sourceFolder": source_folder,
            "creationTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "type": "raw",
            "techniques": metadata.get('techniques',[]),
            "instrumentId": instrument.get("pid",""),
            "sampleId" : sample.get('sampleId',''),
            "proposalId": proposal.get("proposalId",''),
        },
        **ownable
    )



def flatten_metadata(inMetadata,prefix=""):
    outMetadata={}

    for k,v in inMetadata.items():
        nk = '_'.join([i for i in [prefix,k] if i])
        nk = re.sub('_/|/:|/|:',"_",nk)
        if isinstance(v,dict):
            outMetadata = {**outMetadata,**flatten_metadata(v,nk)}
        else:
            outMetadata[nk] = v

    return outMetadata



def prepare_metadata(inMetadata):
    outMetadata = {}

    for k,v in inMetadata.items():
        outMetadata[k] = {
            'value' : v if isinstance(v,str) or isinstance(v,int) or isinstance(v,float) else str(v),
            'unit' : ''
        }
    return outMetadata



def create_orig_datablock(
    dataset_pid: str, 
    file_size: int, 
    file_name: str,
    ownable: pyScClient.Ownable
) -> dict:
    return pyScModel.OrigDatablock(
        **{
            "id" : str(uuid.uuid4()),
            "size": file_size,
            "datasetId": dataset_pid,
            "dataFileList": [
                {
                    "path": file_name,
                    "size": file_size,
                    "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                }
            ],
        },
        **ownable
    )



def get_proposal_id(hdf_structure_string: str, default: str = "") -> dict:
    # extract proposal id from hdf_structure field
    # if such field does not exists, it uses the default

    try:
        # convert json string to dictionary
        hdf_structure_dict = json.loads(
            hdf_structure_string.replace("\n","")
        )

        # now it finds the proposal id which is saved under the key experiment_identifier
        return get_nested_value(
            hdf_structure_dict,
            [
                "children",
                ("children", "name", "entry"),
                ("config", "module","dataset"),
                (None,"name","experiment_identifier","values")
            ]
        )

    except:
        return default



def get_nested_value(structure: dict, path: list):
    # get key
    key = path.pop(0)
    if type(key,str):
        for i in structure[key]:
            temp = get_nested_value(i,path)
            if temp is not None:
                return temp
        return None
    elif type(key,tuple):
        # check the condition
        if key[0] is not None:
            if (key[1] in structure.keys()) and (structure[key[1]] == key[2]):
                for i in structure[key[0]]:
                    temp = get_nested_value(i,path)
                    if temp is not None:
                        return temp
        else:
            if (key[1] in structure.keys()) and (structure[key[1]] == key[2]):
                return structure[key[3]]
        return None
    else:
        raise(Exception("Invalid path"))


# define arguments
parser = argparse.ArgumentParser()

parser.add_argument(
    '-c','--cf','--config','--config-file',
    default='config.json',
    dest='config_file',
    help='Configuration file name. Default": config.json',
    type=str
)
parser.add_argument(
    '-v','--verbose',
    dest='verbose',
    help='Provide logging on stdout',
    action='store_true'
)
parser.add_argument(
    '--file-log',
    dest='file_log',
    help='Provide logging on file',
    action='store_true'
)
parser.add_argument(
    '--debug',
    dest='debug_level',
    help='Adjust the debug level',
    default='INFO',
    type=str
)
parser.set_defaults(verbose=False)
parser.set_defaults(file_log=False)

if __name__ == "__main__":

    # get input argumengts
    args = parser.parse_args()

    # get configuration
    config = get_config(args.config_file)
    
    # define log level
    if args.debug_level=='DEBUG':
        logging_level = logging.DEBUG
    elif args.debug_level=='INFO':
        logging_level = logging.INFO
    elif args.debug_level=='WARN':
        logging_level = logging.WARN
    elif args.debug_level=='ERROR':
        logging_level = logging.ERROR
    elif args.debug_level=='CRITICAL':
        logging_level = logging.CRITICAL

    # instantiate logger
    logger = logging.getLogger('esd extract parameters')
    logger.setLevel(logging_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if args.file_log:
        fh = logging.FileHandler(
            os.path.join(
                config.data_folder,
                config.output_file_name_base+'.log'
            ),
            mode='w', 
            encoding='utf-8'
        )
        fh.setLevel(logging_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    if args.verbose:
        ch = logging.StreamHandler()
        ch.setLevel(logging_level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    main(config,logger)
