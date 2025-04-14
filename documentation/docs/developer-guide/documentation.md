# Documentation

How to build and update documentation.

We use [``mkdocs``](https://www.mkdocs.org/) to build documentation.

## Build Locally

``tox -e docs`` can build documentation locally but you can also use ``mkdocs`` command to serve/build the documentation.

* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.

!!! note
    CI action uses ``mike`` instead of using ``mkdocs`` directly to build and deploy the documentation site at the same time.
