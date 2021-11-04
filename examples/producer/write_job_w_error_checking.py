#!/usr/bin/env python3

import time
from datetime import datetime, timedelta
import json

from file_writer_control.CommandStatus import CommandState
from file_writer_control.JobHandler import JobHandler
from file_writer_control.JobStatus import JobState
from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
from file_writer_control.WorkerJobPool import WorkerJobPool
from file_writer_control.WriteJob import WriteJob

if __name__ == "__main__":
    #kafka_host = "dmsc-kafka01:9092"
    kafka_host = "dmsc-kafka01.cslab.esss.lu.se:9092"
    command_channel = WorkerCommandChannel(
        "{}/UTGARD_writerCommand".format(kafka_host)
    )
    job_handler = JobHandler(worker_finder=command_channel)
    start_time = datetime.now()
    with open("nexus_config.json", "r") as f:
        nexus_structure = f.read()

    # define metadata
    metadata = {
        "name" : "",
        "description" : "",
        "proposal_id" : 421725,
        "sample_id" : "e-PpW0Ayc",
        "instrument_id" : "4319bbe4-3c92-11ec-a2c8-0fd94dec5ee1",
        "techniques" : [ 
            {
                "pid" : "pid-technique-1",
                "name" : "This our technique number 1"
            },
            {
                "pid" : "pid-technique-2",
                "name" : "This our technique number 1"
            }
        ]
    }

    write_job = WriteJob(
        nexus_structure,
        "{0:%Y}-{0:%m}-{0:%d}_{0:%H}{0:%M}.nxs".format(start_time),
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
