======================
Command Line Interface
======================

The Dremio Client uses the `Click`_ library to generate its command line interface. This is installed as
``dremio_client`` by ``pip``

All of the REST API calls are exposed via the command line interface. To see a list of what is available run:

    .. code-block:: bash

        $ dremio_client --help
        Usage: dremio_client [OPTIONS] COMMAND [ARGS]...

        Options:
          --config DIRECTORY   Custom config file.
          -h, --hostname TEXT  Hostname if different from config file
          -p, --port INTEGER   Hostname if different from config file
          --ssl                Use SSL if different from config file
          -u, --username TEXT  username if different from config file
          -p, --password TEXT  password if different from config file
          --help               Show this message and exit.

        Commands:
          catalog_item    return the details of a given catalog item if...
          delete_catalog  delete a catalog entity given by cid and tag...
          delete_pat      delete a personal access token
          graph           return graph info of PDS or VDS         
          group           return group info
          job_results     return results for a given job id pagenated...
          job_status      Return status of job for a given job id
          pat             return personal access token info for a given...
          query           execute a query given by sql and print...
          reflection      return the reflection set
          reflections     return the reflection set
          refresh_pds     refresh metadata/reflections for a given...
          set_catalog     set a catalog entity with the given json...
          set_pat         create a personal access token
          sql             Execute sql statement and return job id
          tags            returns tags for a given catalog entity id or...
          update_catalog  update a catalog entity (cid) given a json...
          user            return user info
          votes           return reflection votes
          wiki            returns wiki for a given catalog entity id or...
          wlm_queues      return the list of wlm queues
          wlm_rules       return the list of wlm rules

The config directory and a number of other settings can be configured from the command line and overwrite the default
config locations.

Run a Query
-----------

We can run a query on the command line and have the results returned as json. We use `jq`_ to manipulate the json after
it is returned. This could be combined into more complete scripts for maintenance and monitoring.

    .. code-block:: bash

        $ dremio_client query --help
        Usage: dremio_client query [OPTIONS]

          execute a query given by sql and print results as json

        Options:
          --sql TEXT  sql query to execute.  [required]
          --help      Show this message and exit.
        $ dremio_client query --sql 'select * from sys.options' | jq '. | length'
        477
        $ dremio_client query --sql 'select * from sys.options' | jq
        [
          {
            "name": "vote.release.leadership.ms",
            "kind": "LONG",
            "type": "SYSTEM",
            "status": "DEFAULT",
            "num_val": 126000000,
            "string_val": null,
            "bool_val": null,
            "float_val": null
          },
          {
            "name": "vote.schedule.millis",
            "kind": "LONG",
            "type": "SYSTEM",
        ...
        $ dremio_client query --sql 'select * from sys.options' | jq 'to_entries[] | "\(.value | .name)"'
        "acceleration.orphan.cleanup_in_milliseconds"
        "accelerator.enable.subhour.policies"
        "accelerator.enable_agg_join"
        ...

.. _Click: https://click.palletsprojects.com
.. _jq: https://stedolan.github.io/jq/
