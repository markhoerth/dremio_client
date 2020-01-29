=========
Utilities
=========

We provide several useful utilities to help with common tasks

Async and sync queries
----------------------

To execute a query synchronously run the following:

    .. code-block:: python

        from dremio_client import init
        import pandas as pd
        query = 'select * from sys.options'
        client = init(simple_client=True)
        results = client.query(self, query)
        pd.DataFrame(results)

This will submit the sql query to the ``sql`` endpoint then poll the ``job-status`` endpoint until the job completed.
If the job has succeeded a ``list`` of ``dicts`` is returned else it throws an exception.

One can execute many queries at once via async querys also:

    .. code-block:: python

        from dremio_client import init
        from concurrent.futures.thread import ThreadPoolExecutor
        from concurrent.futures import as_completed

        queries = ['select * from table0',
                   'select * from table1',
                   'select * from table2',
                   'select * from table3',
                   'select * from table4',
                   'select * from table5'
                   ]
        client = init(simple_client=True)

        def execute(query):
            client.query(query, asynchronous=True)

        # example shamelessly taken from https://docs.python.org/3/library/concurrent.futures.html
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(execute, query): query for query in queries}
            for future in as_completed(futures):
                query = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
                else:
                    print('%r returned %d rows' % (url, len(results)))
