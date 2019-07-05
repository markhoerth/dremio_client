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


.. _confuse: https://github.com/beetbox/confuse
.. _command line interface: ./command_line_interface.html
