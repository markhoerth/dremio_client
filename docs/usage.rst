=====
Usage
=====

To use Dremio client in a project::

    import dremio_client

Configuration
-------------

The Dremio Client is configured using the `confuse`_ yaml based configuration library. This looks for a configuration
file called ``config.yaml`` in:

* macOS: ``~/.config/dremio_client`` and ``~/Library/Application Support/dremio_client``
* Other Unix: ``~/.config/dremio_client`` and ``/etc/dremio_client``
* Windows: ``%APPDATA%\dremio_client`` where the `APPDATA` environment variable falls
  back to ``%HOME%\AppData\Roaming`` if undefined
* Via the environment variable ``DREMIO_CLIENTDIR``

The default config file is as follows:

    .. code-block:: yaml

        auth:
            type: basic #  currently only basic is supported
            username: dremio
            password: dremio123
            timeout: 10
        hostname: localhost
        port: 9047
        ssl: false

The `command line interface`_ can be configured with most of the above parameters via flags or by setting a config directory.
The relevant configs can also be set via environment variables. These take precedence. The environment variable format is
to append ``DREMIO_`` to a config parameter and nested configs are separated by a *_*. For example:
``DREMIO_AUTH_TIMEOUT`` maps to ``auth.timeout`` in the default configuration file above.

You can download the default example `config.yaml`_ from the repository.

Follow these steps to setup the configuration file.

    .. code-block:: bash

        $ mkdir -p ~/.config/dremio_client
        $ wget -O ~/.config/dremio_client/config.yaml https://raw.githubusercontent.com/rymurr/dremio_client/master/dremio_client/conf/config_default.yaml

Once the file is in the directory, please edit the file with the appropriate information.

Autocompletion
--------------

To activate the autocompletion for ``bash`` or ``zsh``, append the following line to the appropriate configuration file.

zsh
^^^

Configuration file name ``~/.zshrc``.

    .. code-block:: bash

        # Enable dremio_client autocomplete
        eval "$(_DREMIO_CLIENT_COMPLETE=source_zsh dremio_client)"

bash
^^^^

Configuration file name ``~/.bashrc``.

    .. code-block:: bash

        # Enable dremio_client autocomplete
        eval "$(_DREMIO_CLIENT_COMPLETE=source_bash dremio_client)"

.. _confuse: https://github.com/beetbox/confuse
.. _command line interface: ./command_line_interface.html
.. _config.yaml: https://github.com/rymurr/dremio_client/blob/master/dremio_client/conf/config_default.yaml
