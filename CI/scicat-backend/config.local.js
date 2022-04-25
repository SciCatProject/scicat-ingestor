"use strict";

let p = require("../package.json");
let version = p.version.split(".").shift();
module.exports = {
  restApiRoot: "/api" + (version > 0 ? "/v" + version : ""),
  host: process.env.HOST || "0.0.0.0",
  port: process.env.PORT || 3000,
  pidPrefix: "20.500.12269",
  publicURLprefix: "https://doi.esss.se/detail/",
  doiPrefix: "10.5072",
  // metadataKeysReturnLimit: 100,
  logbook: {
    enabled: false,
    baseUrl: "localhost:3030/scichatapi",
    username: "logbookReader",
    password: "logrdr"
  },
  datasetReductionEnabled: false,
  reductionKafkaBroker: "kafka:9092",
  reductionKafkaInputTopic: "reduce_input",
  reductionKafkaOutputTopic: "reduce_output",
  grayLog: {
    enabled: false,
    host: "it-graylog.esss.lu.se",
    port: 12201,
    facility: "DMSC",
    owner: "scicat",
    service: "catamel",
  },
  rabbitmq: {
    enabled: false,
    host: null,
    port: null,
    queue: null,
  },
  policyPublicationShiftInYears: 3,
  policyRetentionShiftInYears: 10,
  registerMetadataUri: "https://mds.test.datacite.org/metadata",
  registerDoiUri: "https://mds.test.datacite.org/doi",
  site: "ESS",
  facilities: ["loki"],
  datasetStatusMessages: {
    datasetCreated: "Dataset created",
    datasetOndisk: "Stored on primary disk and on archive disk",
    datasetOnDiskAndTape: "Stored on primary disk and on tape",
    datasetOnTape: "Stored only in archive",
    datasetRetrieved: "Retrieved to target disk",
    datasetDeleted: "Deleted from archive and disk",
  },
  datasetTransitionMessages: {
    scheduleArchive: "Scheduled for archiving",
    schedulePurgeFromDisk: "Scheduled for purging from primary disk",
    scheduleRetrieve: "Scheduled for retrieval",
    scheduleDelete: "Scheduled for removal from archive",
  },
  jobMessages: {
    jobSubmitted: "Submitted for immediate execution",
    jobSubmittedDelayed: "Submitted for delayed execution",
    jobForwarded: "Forwarded to archive system",
    jobStarted: "Execution started",
    jobInProgress: "Finished by %i percent",
    jobSuccess: "Successfully finished",
    jobError: "Finished with errors",
    jobCancel: "Cancelled",
  },
};
