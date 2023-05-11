# Testing

## Testing the file ingestor locally on your machine from src

In order to test the ingestor locally, you need to have an instance of kafka running locally and proper instance of [User Office](https://github.com/UserOfficeProject/user-office-core) and [SciCat Backend v4.x](https://github.com/SciCatProject/scicat-backend-next) accessible.  
  
Create your own local copy of the configuration file config.json.local.test.src from the template config.json.local.test.src,template.  
Add in the information for your local User Office and SciCat instance that you are going to use for testing.  
THis file assumes that you are running a dockerize kafka instance as it is configured in the docker compose file available under the docker folder.  
  
  
Run local kafka containers  
```commandline
    > cd docker  
    > docker-compose -f docker-compose-kafka-with-ui.yml up  
```
This command will not release the terminal, so we can see all the logging from the containers.  
If we want to run it in the background, add the `-d` option:
```commandline
    > docker-compose -f docker-compose-kafka-with-ui.yml -d up  
```

___old setup___  
Create and run the generator container which simulate the filewriter end message to the kafka topic:  
```commandline
    > docker run -d 
        --name scicat-filewriter-generator 
        -v ./sfi_generator_config_docker_local.json:/app/sfi_generator_config.json 
        --network=host 
        scicat-filewriter-ingest-generator:latest
```
Start the jupyter lab with the generator notebook, which allows to simulate the filewriter done writing message.
```commandline
    > cd generator
    > micromamba run -n sfi-gen jupyter lab
```
This assume that the micromamba environment sfi-gen has already been created and is available in your system.
To create the sfi-gen under the micromamba tool, use the following command:
```commandline
    > micromamba env create -n sfi-gen -f requirements_sfi_gen.yaml
```

The following command will start the ingestor. Make sure that you select the correct configuration file
```commandline
    > micromamba run -n sfi python online_ingestor.py --config-file configs/config.json.local.test.src -v --debug DEBUG
```
This command assumes that you have the correct environment setup.
To create the sfi environment, run this command in the command line
```commandline
    > micromamba env create -n sfi -f requirements_sfi.yaml
```

Now you are ready to generate messages that will be ingested. Open a new terminal and run the following command anytime you want to simulate a filewriter message:
```bash
    > docker start scicat-filewriter-ingest-generator
```



