# Documentation

How to build and update documentation.

We use [``mkdocs``](https://www.mkdocs.org/) to build documentation.

## Build Locally

``pixi docs`` can build documentation locally but you can also use ``mkdocs`` command to serve/build the documentation.

* `pixi run -e docs mkdocs serve` - Start the live-reloading docs server.
* `pixi run -e mkdocs build` - Build the documentation site.

!!! note
    CI action uses ``mike`` instead of using ``mkdocs`` directly to build and deploy the documentation site at the same time.


!!! note
    You can also simply use pixi shell and run the `mkdocs` command directly like this:

    ```bash
    pixi shell -e docs  # similar to ``conda activate``.
    mkdocs build  # Now you're in the pixi `docs` environment.
    mkdocs serve  # Same as `pixi run -e docs mkdocs serve`.
    ```
