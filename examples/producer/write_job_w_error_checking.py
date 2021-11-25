#!/usr/bin/env python3

import time
from datetime import datetime, timedelta
import json
import uuid

from file_writer_control.CommandStatus import CommandState
from file_writer_control.JobHandler import JobHandler
from file_writer_control.JobStatus import JobState
from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
from file_writer_control.WorkerJobPool import WorkerJobPool
from file_writer_control.WriteJob import WriteJob

# constants used throughout the generator
kafka_host = "dmsc-kafka01.cslab.esss.lu.se:9092"
channel_name = "{}/UTGARD_writerCommand".format(kafka_host)
run_id = str(uuid.uuid4()).split('-')[0]
run_name = "Run " + run_id
instrument_name = "Ymir"
instrument_id = "4319bbe4-3c92-11ec-a2c8-0fd94dec5ee1"
proposal_id = 421725
sample_id = "e-PpW0Ayc"
start_time = datetime.now()
formatted_start_time = "{0:%Y}{0:%m}{0:%d}{0:%H}{0:%M}{0:%S}".format(start_time)


if __name__ == "__main__":
    #kafka_host = "dmsc-kafka01:9092"
    command_channel = WorkerCommandChannel(channel_name)
    job_handler = JobHandler(worker_finder=command_channel)
    
    file_name = "{}_{}_{}_{}_user_notes.nxs".format(
        instrument_name,
        proposal_id,
        run_id,
        formatted_start_time)
    with open("nexus_config.json", "r") as fh:
        nexus_structure = json.dumps(json.load(fh),separators=(',',':'))


    print("starting run {}".format(run_id))

    # define metadata
    # using MVMS which are defined in the following ticket: https://jira.esss.lu.se/browse/SWAP-1831
    # at least for now :)
    metadata = {
        "proposal_id" : proposal_id,
        "sample_id" : sample_id,
        "run_name" : run_name,
        "run_id" : run_id,
        "instrument_id" : instrument_id,
        "instrument_name" : instrument_name,
        "start_time" : formatted_start_time,
        "techniques" : [ 
            {
                "pid" : "pid-technique-1",
                "name" : "This our technique number 1"
            },
            {
                "pid" : "pid-technique-2",
                "name" : "This our technique number 2"
            }
        ]
    }

    write_job = WriteJob(
        nexus_structure,
        file_name,
        kafka_host,
        start_time,
        metadata=json.dumps(metadata)
    )

    print("Starting write job")
    start_handler = job_handler.start_job(write_job)
    while not start_handler.is_done():
        print("Waiting: {}".format(start_handler.get_state()))
        if start_handler.get_state() == CommandState.ERROR:
            print(
                "Got error when starting job. The error was: {}".format(
                    start_handler.get_message()
                )
            )
            exit(-1)
        elif start_handler.get_state() == CommandState.TIMEOUT_RESPONSE:
            print("Failed to start write job due to start command timeout.")
            exit(-1)
        time.sleep(1)
    print("Write job started")
    stop_time = start_time + timedelta(seconds=60)
    print("Setting stop time")
    stop_handler = job_handler.set_stop_time(stop_time)

    while not stop_handler.is_done():
        time.sleep(1)
        if stop_handler.get_state() == CommandState.ERROR:
            print(
                f"Got error when starting job. The error was: {start_handler.get_message()}"
            )
            exit(-1)
        elif stop_handler.get_state() == CommandState.TIMEOUT_RESPONSE:
            print("Failed to set stop time for write job due to timeout.")
            exit(-1)
    print("Stop time has been set")
    print("Waiting for write job to finish")
    while not job_handler.is_done():
        if job_handler.get_state() == JobState.ERROR:
            print(f"Got job error. The error was: {job_handler.get_message()}")
            exit(-1)
        time.sleep(1)
    print("Write job finished")
