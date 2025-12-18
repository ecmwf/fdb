Examples
#############

This example demonstrates how to use common functionality of the ``PyFDB``. 
The terms ``PyFDB`` and ``FDB`` are sometimes used interchangeably.

PyFDB Initalisation
*******************

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    fdb = pyfdb.FDB()

If no configuration is supplied ``FDB`` falls back to derive the configuration location from
a pre-defined set of locations. You can supply a custom location by specifying the ``FDB_HOME`` environment
variable. If you want to set the location of the configuration file only, use the ``FDB5_CONFIG_FILE`` environment variable.
There is a plethora of different configuration options, if in doubt, refer to the
official ``FDB`` documentation at `ReadTheDocs <https://fields-database.readthedocs.io/en/latest/>`__.

You can also pass (dynamically) created custom configurations as parameters to the ``FDB`` constructor. 
Those can be supplied as a ``Path`` pointing to the location
of the configuration file, as a ``str`` which is the ``yaml`` representation of the configuration or as
a ``Dict[str, Any]`` as shown below. 

.. code-block:: python

    config = {
        "type":"local",
        "engine":"toc",
        "schema":"/path/to/fdb_schema",
        "spaces":[
            {
                "handler":"Default",
                "roots":[
                    {"path": "/path/to/root"}
                ]
            }
        ],
    }
    fdb = pyfdb.FDB(config=config, user_config={})

.. clear-namespace

The different method of the ``FDB`` class can be leverage for different use-cases. Below we listed
examples of the most common method class display different ways of using the Python API.

Archive
***********

**Archive binary data into the underlying FDB.**

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)
    filename = data_path / "x138-300.grib"

    fdb.archive(filename.read_bytes())
    fdb.flush()

In this scenario a ``GRIB`` file is archived to the configured ``FDB``. The FDB reads metadata from
the given ``GRIB`` file and saves this, if no optional ``identifier`` is supplied. If we set an
``identifier``, there are no consistency checks taking place and our data is saved with the metadata
given from the supplied ``identifier``.

The ``flush`` command guarantees that the archived data has been flushed to the ``FDB``.

.. clear-namespace

Flush
***********

**Flush all buffers and close all data handles of the underlying FDB into a consistent DB state.**

.. invisible-code-block: python

   import pyfdb

.. tip::

   It's always safe to call ``flush``.

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read())
    fdb.flush()

The ``flush`` command guarantees that the archived data has been flushed to the ``FDB``.

.. clear-namespace

Retrieving
***********

**Retrieve data which is specified by a MARS selection.**

.. invisible-code-block: python

   import pyfdb

To Memory
=========

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": "167/165/166",
        "time": "1800",
    }

    with fdb.retrieve(selection) as data_handle:
        data_handle.read(4) # == b"GRIB"
        # data_handle.readall() # As an alternative to read all messages

The code above shows how to retrieve a MARS selection given as a dictionary. The retrieved ``data_handle``
has to be opened before being read and closed afterwards. If you are interested in reading the entire
``data_handle``, you could use the ``readall`` method. 

.. tip::

    For the ``readall`` method there is no need
    to ``open`` or ``close`` the ``data_handle`` after the call to ``readall``.

.. clear-namespace

To File
=========

Another use-case, which is often needed, is saving certain ``GRIB`` data in a file on your local machine.
The following code is showing how to achieve this:

.. code-block:: python

    import shutil

    fdb = pyfdb.FDB(fdb_config_path)

    # Specify selection here
    # --------------------
    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": "167/165/166",
        "time": "1800",
    }

    filename = test_case_tmp / 'output.grib'

    with open(filename, 'wb') as out:
        with fdb.retrieve(selection) as data_handle:
            out.write(data_handle.readall())

.. clear-namespace

List
****

**List data present at the underlying fdb archive and which can be retrieved.**

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "param": "167/131/132",
            "time": "1800",
        }

    list_iterator = fdb.list(selection, level=1)
    elements = list(list_iterator)

    for el in elements:
        print(el)

    assert len(elements) == 1

The code above shows an example of listing the contents of the ``FDB`` for a given selection.
The selection is a class describing a ``MarsSelection`` (A MARS request without the verb).

.. note::

   A ``MarsSelection`` doesn't need to be fully specified. In the example above you can see that
   the param key contains a list of values. Contrary to an Identifier, which is a fully specified 
   ``MarsSelection`` and therefore only contains singular values, even ``x/to/y/by/z`` ranges are allowed.


Depending on the given ``level`` different outputs are to be expected:

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }
    list_iterator = fdb.list(selection) # level == 3
    elements = list(list_iterator)
    print(elements[0])

::

    {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g}
    {type=an,levtype=sfc}
    {step=0,param=131},
    TocFieldLocation[uri=URI[scheme=file,name=/<path-to-db_store>/ea:0001:oper:20200101:1800:g/an:sfc.20251118.151917.<?>.375861178007828.data],offset=10732,length=10732,remapKey={}],
    length=10732,
    timestamp=1763479157

    {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g}
    {type=an,levtype=sfc}
    {step=0,param=132},
    TocFieldLocation[uri=URI[scheme=file,name=/<path-to-db_store>/db_store/ea:0001:oper:20200101:1800:g/an:sfc.20251118.151917.<?>.375861178007828.data],offset=21464,length=10732,remapKey={}],
    length=10732,
    timestamp=1763479157

    {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g}
    {type=an,levtype=sfc}
    {step=0,param=167},
    TocFieldLocation[uri=URI[scheme=file,name=/<path-to-db_store>/db_store/ea:0001:oper:20200101:1800:g/an:sfc.20251118.151917.<?>.375861178007828.data],offset=0,length=10732,remapKey={}],
    length=10732,
    timestamp=1763479157

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)
    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }
    list_iterator = fdb.list(selection, level=2)
    elements = list(list_iterator)
    print(elements[0])

::

    {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g}
    {type=an,levtype=sfc},
    length=0,
    timestamp=0

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)
    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }
    list_iterator = fdb.list(selection, level=1)
    elements = list(list_iterator)
    print(elements[0])

:: 

    {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g},
    length=0,
    timestamp=0

For each level the returned iterator of ``ListElement`` is restricting the elements to the corresponding
level of the underlying FDB. If you specify ``level=3``, the returned ``ListElement`` contains a valid
``DataHandle`` object. You can use this directly to read the message represented by the ``ListElement``, e.g.:


.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    list_iterator = fdb.list(selection, level=3)
    selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "param": "167/131/132",
            "time": "1800",
    }
    elements = list(list_iterator)

    for el in elements:
        data_handle = el.data_handle()
        data_handle.open()
        assert data_handle.read(4) == b"GRIB"
        data_handle.close()

.. clear-namespace

Inspect
*******

**Inspects the content of the underlying FDB and returns a generator of list elements
describing which field was part of the MARS selection.**

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    identifier = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": "131",
        "time": "1800",
    }

    inspect_iterator = fdb.inspect(identifier)
    elements = list(inspect_iterator)

    # Because the identifier needs to be fully specified, there
    # should be only a single element returned
    assert len(elements) == 1

    for el in elements:
        with el.data_handle() as data_handle:
            assert data_handle.read(4) == b"GRIB"

The code above shows how to inspect certain elements stored in the ``FDB``. This call is similar to
a ``list`` call with ``level=3``, although the internals are quite different. The functionality is
designed to list a vast amount of individual fields. 

Similar to the ``list`` command, each ``ListElement`` returned, contains a ``DataHandle`` which can
be used to directly access the data associated with the element, see the example of ``list``.

.. note::

   Due to the internals of the ``FDB`` only a fully specified MARS selection (also called Identifier) is accepted.
   If a range for a key is given, e.g. ``param=131/132``, the second value is silently dropped.

.. clear-namespace


Status
*******

**List the status of all FDB entries with their control identifiers, e.g., whether a certain database was locked for retrieval.**

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
    }

    status_iterator = fdb.status(selection)
    elements = list(status_iterator)

    len(elements) # == 32

The output of such a command can look like the above and is the same output you get from the
call to ``control`` when setting certain ``ControlIdentifiers`` for elements of the ``FDB``.

::

   ControlIdentifiers[4],
   {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g},
   URI[scheme=toc,name=/<path-to-db-store>/ea:0001:oper:20200101:1800:g].


You can see that the ``ControlIdentifier`` with value ``4`` is active for the given entry of the ``FDB``.
This corresponds to the ``ControlIdentifier.ARCHIVE`` value. 

.. tip::
   Use the ``control`` functionality of FDB to switch certain properties of ``FDB`` elements.
   Refer to the :ref:`control_label` section for further information.

Wipe
*******

**Wipe data from the database**

.. invisible-code-block: python

   import pyfdb

Delete FDB databases and the data therein contained. Use the passed
selection to identify the database to delete. This is equivalent to a UNIX rm command.
This function deletes either whole databases, or whole indexes within databases

.. tip::

   It's always advised to check the elements of a deletion before running it with the ``doit`` flag.
   Double check that the dry-run, which is active per default, really returns the elements you are
   expecting.

A potential deletion operation could look like this:

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    elements = list(fdb.wipe({"class": "ea"}))
    len(elements) > 0

    # NOTE: Double check that the returned elements are those you want to delete
    for element in elements:
        print(element)
    

    # Do the actual deletion with the `doit=True` flag
    wipe_iterator = fdb.wipe({"class": "ea"}, doit=True)
    wiped_elements = list(wipe_iterator)

    for element in elements:
        print(element)

.. clear-namespace

Move
*******

**Move content of one FDB database to a new URI.**

.. invisible-code-block: python

   import pyfdb

This locks the source database, make it possible to create a second
database in another root, duplicates all data.
Source data is moved.

.. warning::

   The last element of the iterator is containing a ``MoveElement`` which
   resembles the folder the database is contained in. This element is normally used
   to delete the containing folder of the initial ``FDB``. Handle this explicitly by
   dropping the last element from the iterator as shown in the example below.

.. tip::

   If the move command fails or doesn't return the expected result, check whether you
   specified the path, to which you want to move the database, as a root in the new ``FDB``.

   Also make sure, that you called the ``execute`` function on each of the returned ``MoveElement`` s.

.. code-block:: python

    from pathlib import Path
    import yaml

    def add_new_root(fdb_config_path: Path, new_root: Path) -> str:
        # Manipulate the config of the FDB to contain a new root
        fdb_config = yaml.safe_load(fdb_config_path.read_text())

        fdb_config["spaces"][0]["roots"].append({"path": str(new_root)})

        new_root.mkdir()

        fdb_config_string = yaml.dump(fdb_config)
        fdb_config_path.write_text(fdb_config_string)

        return fdb_config_string

    new_root: Path = fdb_config_path.parent / "new_db"
    updated_config = add_new_root(fdb_config_path, new_root)

    fdb = pyfdb.FDB(updated_config)

    # Selection for the second level of the schema
    selection = {
        "class":"ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    move_iterator = fdb.move(
        selection,
        pyfdb.URI.from_str(str(new_root)),
    )

    elements = []

    for el in move_iterator:
        print(el)
        elements.append(el)

    # Only iterate of all but the last element.
    # The last element is used from the fdb-move tool to trigger the deletion of the src folder
    # This is not what we want in this regard.
    for el in elements[:-1]:
        el.execute()

    assert len(list(new_root.iterdir())) == 1

The example above shows how a database in a preexisting ``FDB`` can be moved. In this case a
preexisting ``FDB`` configuration file is loaded and an additional root path is added. It's important
to mention that the move command operates on root folders of ``FDB`` instances.

After specifying the selection we want to move, this has to be an selection which contains keys of 
the first and second level of the schema, we can call the ``move`` function. The returned elements
can be iterated and the actual move operation can be triggered by calling ``execute`` on each element.

.. clear-namespace

Purge
*******
**Remove duplicate data from the database.**

.. invisible-code-block: python

   import pyfdb

Purge duplicate entries from the database and remove the associated data if the data is owned and not adopted.
Data in the ``FDB`` is immutable. It is masked, but not removed, when overwritten with new data using the same key.
Masked data can no longer be accessed. Indexes and data files that only contains masked data may be removed.

If an index refers to data that is not owned by the FDB (in particular data which has been adopted from an
existing ``FDB``), this data will not be removed.

.. tip::

   It's always advised to check the elements of a deletion before running it with the ``doit`` flag.
   Double check that the dry-run, which is active per default, really returns the elements you are
   expecting.

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    elements = list(fdb.purge({"class": "ea"}))
    len(elements) > 0

    # NOTE: Double check that the returned elements are those you want to delete
    for element in elements:
        print(element)

    # Do the actual deletion with the `doit=True` flag
    purge_iterator = fdb.purge({"class": "ea"}, doit=True)
    purge_elements = list(purge_iterator)

    for element in purge_elements:
        print(element)

.. clear-namespace

Stats
*******
**Print information about FDB databases, aggregating the information over all the databases visited into a final summary.**

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "param": "167/165/166",
            "time": "1800",
        }

    elements = list(fdb.stats(selection))

    for el in elements:
        print(el)

The example above shows how to use the ``stats`` function to get an overview over the statistics a given MARS selection
has. For every database and every index the selection touches, it aggregates statistics and shows the result in a table.
The ``StatsElement`` s returned from the call are Python string resembling individual lines of the output generated by
the underlying ``FDB``. A potential call of the example above could lead to the following output:

::

    Index Statistics:
    Fields                          : 3
    Size of fields                  : 32,196 (31.4414 Kbytes)
    Reacheable fields               : 3
    Reachable size                  : 32,196 (31.4414 Kbytes)

    DB Statistics:
    Databases                       : 1
    TOC records                     : 2
    Size of TOC files               : 2,048 (2 Kbytes)
    Size of schemas files           : 228 (228 bytes)
    TOC records                     : 2
    Owned data files                : 1
    Size of owned data files        : 32,196 (31.4414 Kbytes)
    Index files                     : 1
    Size of index files             : 131,072 (128 Kbytes)
    Size of TOC files               : 2,048 (2 Kbytes)
    Total owned size                : 165,544 (161.664 Kbytes)
    Total size                      : 165,544 (161.664 Kbytes)

.. _control_label:

.. clear-namespace

Control
*******
**Enable certain features of FDB databases, e.g., disables or enables retrieving, list, etc.**

Under certain circumstances

.. invisible-code-block: python

   import pyfdb

.. tip::
   Consume the iterator, returned by the ``control`` call, completely. Otherwise, the lock file
   won't be created.

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    print("Retrieve without lock")
    with fdb.retrieve(
        {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "param": "167/165/166",
            "time": "1800",
        }
    ) as data_handle:
        data_handle.read(4) # == b"GRIB"

    assert not (
        fdb_config_path.parent
        / "db_store"
        / "ea:0001:oper:20200101:1800:g"
        / "retrieve.lock"
    ).exists()

    print("Locking database for retrieve")

    request = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    control_iterator = fdb.control(
        request,
        pyfdb.ControlAction.DISABLE,
        [pyfdb.ControlIdentifier.RETRIEVE],
    )
    assert control_iterator

    elements = []

    for el in control_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    assert (
        fdb_config_path.parent
        / "db_store"
        / "ea:0001:oper:20200101:1800:g"
        / "retrieve.lock"
    ).exists()

    print("Retrieve with lock")
    with fdb.retrieve(
        {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "param": "167/165/166",
            "time": "1800",
        },
    ) as data_handle:
        data_handle.read(4) # == b"GRIB"

    print("Success")

The example given above shows how the activation/deactivation of the wipe functionality of the ``FDB``
works for a certain selection. 

After specifying the selection we want to target, this has to be a selection which contains keys of 
the first and second level of the schema, we can call the ``control`` function and specify the wished action:
in this case ``ControlIdentifier.WIPE`` and ``ControlAction.DISABLE``, which translate to wanting to disable
wiping for the specified database. We could specify multiple of the ``ControlIdentifier`` in a single call.

For each of the ``ControlIdentifier`` the underlying ``FDB`` will create a ``<control-identifier-name>.lock`` file, 
which resides inside the database specified by the MARS selection. If we decide to enable the action again, this
file gets deleted.

After disabling the action, a call to it results in an empty iterator being returned.

.. clear-namespace

Axes
*******
**Return the 'axes' and their extent of a selection for a given level of the schema in an IndexAxis object.**

If a key is not specified the entire extent (all values) are returned.

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            # "date": "20200101", # Left out to show all values are returned
            "levtype": "sfc",
            "step": "0",
            "time": "1800",
        }

    print("---------- Level 3: ----------")
    index_axis = fdb.axes(selection)
    # len(index_axis.items()) == 11

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    print("---------- Level 2: ----------")
    index_axis = fdb.axes(selection, level=2)
    #len(index_axis.items()) == 8

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    print("---------- Level 1: ----------")
    index_axis = fdb.axes(selection, level=1)
    # len(index_axis.items()) == 6

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

The example above produces the following output:

::

   ---------- Level 3: ----------
   k=class    | v=['ea']
   k=date     | v=['20200101', '20200102', '20200103', '20200104']
   k=domain   | v=['g']
   k=expver   | v=['0001']
   k=levelist | v=['']
   k=levtype  | v=['sfc']
   k=param    | v=['131', '132', '167']
   k=step     | v=['0']
   k=stream   | v=['oper']
   k=time     | v=['1800']
   k=type     | v=['an']

   ---------- Level 2: ----------
   k=class    | v=['ea']
   k=date     | v=['20200101', '20200102', '20200103', '20200104']
   k=domain   | v=['g']
   k=expver   | v=['0001']
   k=levtype  | v=['sfc']
   k=stream   | v=['oper']
   k=time     | v=['1800']
   k=type     | v=['an']

   ---------- Level 1: ----------
   k=class    | v=['ea']
   k=date     | v=['20200101', '20200102', '20200103', '20200104']
   k=domain   | v=['g']
   k=expver   | v=['0001']
   k=stream   | v=['oper']
   k=time     | v=['1800']

For each specified ``level``, the keys affected by the MARS selection at that level are returned. 
Optional keys in the ``FDB`` schema appear as empty lists. If a key is missing from the selection,
the key and all values stored in the ``FDB`` are returned (see the ``date`` key above).

In case you want to see the 'span' of all elements stored in an ``FDB`` you could use the following code:

.. warning::

   This following code is an expensive call (depending on the size of the ``FDB``).
   For testing purposes or locally configured ``FDB`` instances this is fine.

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)
    selection = pyfdb.WildcardMarsSelection()
    index_axis: pyfdb.IndexAxis = fdb.axes(selection)

.. clear-namespace

Enabled
*******
**Check whether a specific control identifier is enabled.**

.. invisible-code-block: python

   import pyfdb

.. code-block:: python

    from pyfdb import ControlIdentifier

    fdb = pyfdb.FDB(fdb_config_path)

    assert fdb.enabled(ControlIdentifier.NONE) is True
    assert fdb.enabled(ControlIdentifier.LIST) is True
    assert fdb.enabled(ControlIdentifier.RETRIEVE) is True
    assert fdb.enabled(ControlIdentifier.ARCHIVE) is True
    assert fdb.enabled(ControlIdentifier.WIPE) is True
    assert fdb.enabled(ControlIdentifier.UNIQUEROOT) is True

The examples above show how a default ``FDB`` is configured, this is, all possible ``ControlAction`` s
are enabled by default.

Configuring the ``FDB`` to disallow writing via setting ``writable = False`` in the ``fdb_config.yaml``,
we end up with the following ``ControlIdentifier`` s:

.. code-block:: python

    import yaml
    from pyfdb import ControlIdentifier

    fdb_config = yaml.safe_load(fdb_config_path.read_text())
    fdb_config["writable"] = False

    fdb = pyfdb.FDB(fdb_config)

    assert fdb.enabled(ControlIdentifier.NONE) is True
    assert fdb.enabled(ControlIdentifier.LIST) is True
    assert fdb.enabled(ControlIdentifier.RETRIEVE) is True
    assert fdb.enabled(ControlIdentifier.ARCHIVE) is False
    assert fdb.enabled(ControlIdentifier.WIPE) is False
    assert fdb.enabled(ControlIdentifier.UNIQUEROOT) is True

The configuration changes accordingly, if we substitute ``writable = False`` with ``visitable = False``.

.. clear-namespace

Needs Flush
*************
**Return whether a flush of the FDB is needed.**

.. code-block:: python

    fdb = pyfdb.FDB(fdb_config_path)

    filename = data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read())
    fdb.needs_flush() # == True
    fdb.flush()
    fdb.needs_flush() # == False

The example above shows return value of the ``flush`` command after an archive command results in ``True``. 
Flushing resets the internal status of the ``FDB`` and the call to ``needs_flush`` returns ``False`` afterwards.

.. clear-namespace
