FROM python:3.12-slim

# Install system packages necessary for HDF5, building dependencies, and git
RUN apt-get update && apt-get install -y --no-install-recommends \
    libhdf5-dev build-essential git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the requirements directory into the container
COPY requirements/ ./requirements/

# Install dependencies from the base requirements file
RUN pip install --upgrade pip && pip install -r requirements/base.in && pip install python-dateutil

# Copy the full application code
COPY . .

# Set a pretend version for setuptools-scm to use
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0

# Install your package in editable mode
RUN pip install -e .

# By default, run the ingestor
CMD ["sh", "-c", "scicat_background_ingestor --logging.verbose -c ${CONFIG_FILE} --nexus-file ${NEXUS_FILE}"]