# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.8-slim-buster

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements_SFI.txt .
RUN python -m pip install -r requirements_SFI.txt

WORKDIR /app
COPY online_ingestor.py /app
COPY ingestor_lib.py /app
COPY user_office_lib.py /app
COPY config_sample.json /app/config.json

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "online_ingestor.py", "--config-file","config.json","-v","--debug","DEBUG"]

