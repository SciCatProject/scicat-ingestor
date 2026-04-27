# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)

# ---- Build stage ----
FROM python:3.12-slim AS builder

ARG VERSION=0.0.0

WORKDIR /build

RUN pip install --no-cache-dir --upgrade pip

COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/

ENV SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION}

RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir ".[app]"

# ---- Final stage ----
FROM python:3.12-slim

LABEL org.opencontainers.image.title="SciCat Ingestor" \
    org.opencontainers.image.description="A daemon that creates SciCat datasets whenever a new file is written by a file-writer." \
    org.opencontainers.image.source="https://github.com/ScicatProject/scicat-ingestor" \
    org.opencontainers.image.licenses="BSD-3-Clause"

RUN groupadd --system ingestor && \
    useradd --system --gid ingestor --create-home --shell /sbin/nologin ingestor

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Health check polls the built-in /health endpoint exposed by the online ingestor.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

USER ingestor

# Health check HTTP server port (configurable via config file).
EXPOSE 8080

ENTRYPOINT ["scicat_ingestor"]
CMD ["--config-file", "/config/config.yml"]
