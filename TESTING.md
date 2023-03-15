# Testing

## Testing the file ingestor locally on your machine from src

In order to test the ingestor locally, you need to have an instance of kafka running locally and proper instance of [User Office](https://github.com/UserOfficeProject/user-office-core) and [SciCat Backend v4.x](https://github.com/SciCatProject/scicat-backend-next) accessible.  
  
Create your own local copy of the configuration file config.json.local.test.src from the template config.json.local.test.src,template.  
Add in the information for your local User Office and SciCat instance that you are going to use for testing.  
THis file assumes that you are running a dockerize kafka instance as it is configured in the docker compose file available under the docker folder.  
  
  
Run local kafka containers  
```bash
    > cd docker  
    > docker-compose up -d  
```

Create the generator container which post a specifically crafted message to the kafka topic:
    > docker run -d --name scicat-filewriter-generator -v ./sfi_generator_config_docker_local.json:/app/sfi_generator_config.json --network=host scicat-filewriter-ingest-generator:latest

Than start the ingestor:
    > conda activate SFI
    > python online_ingestor.py --config-file configs/config.json.local.test.src -v --debug DEBUG

Now you are ready to generate messages that will be ingested. Open a new terminal and run the following command anytime you want to simulate a filewriter message:
    > docker start scicat-filewriter-ingest-generator



