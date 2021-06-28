# scicat-filewriter-ingest


this project listen to the kafka topic command_channel.  
when a new file is written to the file server by the file writer  
it interface with scicat and create a raw dataset  

important messages to listen to are:
- run start (https://github.com/ess-dmsc/streaming-data-types/blob/master/schemas/pl72_run_start.fbs)
- finished writing (https://github.com/ess-dmsc/streaming-data-types/blob/master/schemas/wrdn_finished_writing.fbs)

This is the structure of the two messages as listed in the links above:
### run start
```
RunStart {                                     //  *Mantid*    // *File Writer* // *Description*
    start_time : uint64;                             //  Required    //  Optional     // milliseconds since Unix epoch (1 Jan 1970)
    stop_time : uint64;                              //  Unused      //  Optional     // milliseconds since Unix epoch (1 Jan 1970), optional, can send a RunStop instead
    run_name : string;                               //  Required    //  Unused       // Name for the run, used as workspace name by Mantid
    instrument_name : string;                        //  Required    //  Unused       // Name of the instrument, only required by Mantid
    nexus_structure : string;                        //  Optional    //  Required     // JSON description of NeXus file (See https://github.com/ess-dmsc/kafka-to-nexus/ for detail)
                                                                                      // If present Mantid will parse this to get the instrument geometry, otherwise it will attempt
                                                                                      // to look up an Instrument Definition File (IDF) based on the instrument name
    job_id : string;                                 //  Unused      //  Required     // A unique identifier for the file writing job
    broker : string;                                 //  Unused      //  Required     // Broker name and port, for example "localhost:9092", from which the file writer should get data
    service_id : string;                             //  Unused      //  Optional     // The identifier for the instance of the file-writer that should handle this command
    filename : string;                               //  Unused      //  Required     // Name of the file to write, for example run_1234.nxs
    n_periods : uint32 = 1;                          //  Optional    //  Unused       // Number of periods (ISIS only)
                                                                                      // Periods provide a way to segregate data at the data acquisition stage
    detector_spectrum_map: SpectraDetectorMapping;   //  Optional    //  Unused       // Map spectrum numbers in the event messages to detector IDs in the instrument definition (optional, for ISIS only)
    metadata : string;                               //  Unused      //  Optional     // Holds a JSON string with (static) metadata about the measurement. E.g. proposal id.
}
```
### finished writing
```
table FinishedWriting {
    service_id : string (required);     // milliseconds since Unix epoch (1 Jan 1970). When set to 0, will trigger a "stop NOW" code path in the file writer.
    job_id : string (required);         // The unique identifier of the file writing job
    error_encountered : bool;           // True if stopped due to error
    file_name : string (required);      // Name of file that was just closed.
    metadata : string;                  // JSON string with metadata about the file that was just closed.
    message : string;                   // Must hold an error message if filewriting was stopped due to an error, is optional otherwise.
}
```

We need to decide if we are going to listen only to "finished writing" or also to "run start" and populate the scicat metadata from both.

There are few libraries already available that can be used to list and interpret the messages received:
- https://github.com/ess-dmsc/python-streaming-data-types
- https://github.com/ess-dmsc/file-writer-control

Following is an example of a listener code:
- https://github.com/ess-dmsc/file-writer-control/blob/master/examples/list_status.py

This link provide a lot of information regarding the setup of the kafka cluster and broker:
- https://confluence.esss.lu.se/pages/viewpage.action?pageId=167903470

In the folder examples/producer, a file producer application is provided to test the consumer
