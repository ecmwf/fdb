Example usage
=============

This example demonstrates how to archive, list, and retrieve GRIB files.
Requires: FDB5, shutil, eccodes‑python, pyfdb, and test GRIB files 
(in `tests/pyfdb`) [1](https://github.com/ecmwf/pyfdb/blob/develop/README.md).

.. TODO(TKR): UPDATE THIS DOCUMENTATION

Initializing an FDB client:

.. code-block:: python

    import pyfdb, shutil

    pyfdb = pyfdb.FDB()

You can also pass custom configs:

.. code-block:: python

    config = dict(
        type="local", engine="toc", schema="/path/to/fdb_schema",
        spaces=[dict(handler="Default", roots=[{"path": "/path/to/root"}])],
    )
    fdb = pyfdb.FDB(config=config, userconfig={})

Archiving data:

.. code-block:: python

    key = {
        'domain': 'g', 'stream': 'oper', 'levtype': 'pl',
        'levelist': '300', 'date': '20191110',
        'time': '0000', 'step': '0', 'param': '138',
        'class': 'rd', 'type': 'an', 'expver': 'xxxx'
    }
    with open('x138-300.grib','rb') as f:
        fdb.archive(f.read(), key)
    fdb.flush()

Listing entries:

.. code-block:: python

    request = {
        'class': 'rd', 'expver': 'xxxx', 'stream': 'oper',
        'date': '20191110', 'time': '0000', 'domain': 'g',
        'type': 'an', 'levtype': 'pl', 'step': 0,
        'levelist': [300, '500'], 'param': ['138', 155, 't']
    }
    for el in pyfdb.list(request, True, True):
        print(el['keys'])

Retrieving to file:

.. code-block:: python

    import tempfile, os
    dir = tempfile.gettempdir()
    filename = os.path.join(dir, 'x138-300bis.grib')
    with open(filename, 'wb') as out:
        src = fdb.retrieve(request)
        shutil.copyfileobj(src, out)

Reading into memory:

.. code-block:: python

    datareader = pyfdb.retrieve(request)
    chunk = datareader.read(10)
    datareader.seek(0)

