=============
Dremio client
=============


.. image:: https://img.shields.io/pypi/v/dremio_client.svg
        :target: https://pypi.python.org/pypi/dremio_client

.. image:: https://img.shields.io/travis/rymurr/dremio_client.svg
        :target: https://travis-ci.org/rymurr/dremio_client

.. image:: https://readthedocs.org/projects/dremio-client/badge/?version=latest
        :target: https://dremio-client.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/rymurr/dremio_client/shield.svg
     :target: https://pyup.io/repos/github/rymurr/dremio_client/
     :alt: Updates

.. image:: https://img.shields.io/codacy/grade/78c83e3484634e32b17496e0dbe7ade5
     :target: https://app.codacy.com/project/rymurr/dremio_client/dashboard
     :alt: Codacy

.. image:: https://img.shields.io/codecov/c/github/rymurr/dremio_client
     :target: https://codecov.io/gh/rymurr/dremio_client
     :alt: Codecov


The **un-official** python client for Dremio's REST API. This enables both administrators and data scientists to get
the most out of `Dremio`_ in Python

.. _Dremio: https://dremio.com

* Documentation: https://dremio-client.readthedocs.io.
* Github: https://github.com/rymurr/dremio_client
* PyPI: https://pypi.python.org/pypi/dremio_client
* Free software: Apache Software License 2.0

Features
--------

* Cross platform support
* All Pythons between 2.7 - 3.7 supported
* Full support for Dremio's `REST API`_
* Optional Support for Dremio's `ODBC`_ or experimental `Arrow Flight`_ capabilities
* Rich config file support via `confuse`_ yaml config library. Simple to create a client (config stored in a yaml file)

    .. code-block:: python

        from dremio_client import init
        client = init() # initialise connectivity to Dremio via config file
        catalog = client.data # fetch catalog
        vds = catalog.space.vds.get() # fetch a specific dataset
        df = vds.query() # query the first 1000 rows of the dataset and return as a DataFrame
        pds = catalog.source.pds.get() # fetch a physical dataset
        pds.metadata_refresh() # refresh metadata on that dataset

* CLI interface for integration with scripts

    .. code-block:: bash

        $ dremio_client query --sql 'select * from sys.options'
        {'results':results}
        $ dremio_client refresh-metadata --table 'my.vds.name'
        {'result':'ok'}

* Catalog autocompletion in Jupyter Notebooks

.. image:: https://raw.github.com/rymurr/dremio_client/master/docs/images/autocomplete.gif


.. _REST API: https://docs.dremio.com/rest-api/
.. _ODBC: https://docs.dremio.com/drivers/dremio-odbc-driver.html
.. _Arrow Flight: https://arrow.apache.org/docs/format/Flight.html?highlight=flight
.. _confuse: https://github.com/beetbox/confuse


Status
------

This is still alpha software and is relatively incomplete. Contributions in the form of Github Issues or Pull requests
are welcome. See `CONTRIBUTING`_

.. _CONTRIBUTING:

TODO
----

* see issues
