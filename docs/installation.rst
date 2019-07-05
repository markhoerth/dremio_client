.. highlight:: shell

============
Installation
============


Stable release
--------------

To install basic Dremio client, run this command in your terminal:

.. code-block:: console

    $ pip install dremio_client

This installs a minimal set of dependencies suitable for only interacting with the REST API and for restricted environments.
To get full set of dependencies run:

.. code-block:: console

    $ pip install dremio_client[noarrow]  # everything but pyarrow
    $ pip install dremio_client[full]  # all dependencies

This is the preferred method to install Dremio client, as it will always install the most recent stable release.

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/

Conda release
-------------

You can use conda to install Dremio Client by doing:

.. code-block:: console

    $ conda install dremio_client -c rymurr -c conda-forge

From sources
------------

The sources for Dremio client can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/rymurr/dremio_client

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/rymurr/dremio_client/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/rymurr/dremio_client
.. _tarball: https://github.com/rymurr/dremio_client/tarball/master
