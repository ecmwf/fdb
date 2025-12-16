Example usage
#############

This example demonstrates how to use common functionality of the ``PyFDB``.

PyFDB Intro
***********

.. code-block:: python

    fdb_config_path = Path("<path-to-fdb-config>")
    
    fdb_config = pyfdb.Config(config_file.read_text())
    pyfdb = pyfdb.PyFDB(fdb_config)

    fdb_config_path = Path("<path-to-fdb-config>")
    fdb_config = pyfdb.Config(config_file.read_text())
    pyfdb = pyfdb.PyFDB(fdb_config)



You can also pass custom configurations. Those can be supplied as a ``Path`` pointing to the location
of the configuration file, as a ``str`` which is the yaml representation of the configuration or as
a ``Dict[str, Any]`` as shown below. 

.. code-block:: python

    config = {
        type="local",
        engine="toc",
        schema="/path/to/fdb_schema",
        spaces=[
            {
                handler="Default",
                roots=[
                    {"path": "/path/to/root"}
                ]
            }
        ],
    }
    fdb = pyfdb.PyFDB(config=config, userconfig={})

.. TODO(TKR): Specify what a user config is and how this can be used here.

Below are some of the common examples you want to use `PyFDB` for:

Archive
***********

**Archive binary data into the underlying FDB.**

.. code-block:: python

    fdb_config = pyfdb.Config(config_file.read_text())
    pyfdb = pyfdb.PyFDB(fdb_config)

    filename = test_data_path / "x138-300.grib"

    pyfdb.archive(open(filename, "rb").read())
    pyfdb.flush()

In this scenario a ``GRIB`` file is archived to the configured ``FDB``. The FDB reads metadata from
the given ``GRIB`` file and saves this, if no optional ``identifier`` is supplied. If we set an
``identifier``, there are no consistency checks taking place and our data is saved with the metadata
given from the supplied ``identifier``.

The ``flush`` command guarantees that the archived data has been flushed to the ``FDB``.

Flush
***********

**Flush all buffers and close all data handles of the underlying FDB into a consistent DB state.**

.. tip::

   It's always safe to call ``flush``.

.. code-block:: python

    fdb_config = pyfdb.Config(config_file.read_text())
    pyfdb = pyfdb.PyFDB(fdb_config)

    filename = test_data_path / "x138-300.grib"

    pyfdb.archive(open(filename, "rb").read())
    pyfdb.flush()

The ``flush`` command guarantees that the archived data has been flushed to the ``FDB``.

Retrieving
***********

**Retrieve data which is specified by a MARS selection.**

To Memory
=========

.. code-block:: python

    fdb_config = Config(config_file.read())
    pyfdb = PyFDB(fdb_config)

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

    data_handle = pyfdb.retrieve(selection)

    data_handle.open()
    data_handle.read(4) # == b"GRIB"
    # data_handle.read_all() # As an alternative to read all messages
    data_handle.close()

The code above shows how to retrieve a MARS selection given as a dictionary. The retrieved ``data_handle``
has to be opened before being read and closed afterwards. If you are interested in reading the entire
``data_handle``, you could use the ``read_all`` method. 

.. tip::

    For the ``read_all`` method there is no need
    to ``open`` or ``close`` the ``data_handle`` after the call to ``read_all``.

To File
=========

Another use-case, which is often needed, is saving certain ``GRIB`` data in a file on your local machine.
The following code is showing how to achieve this:

.. code-block:: python

    import shutil

    # Specify request here
    # --------------------
    data_handle = pyfdb.retrieve(request)

    filename = os.path.join("<directory-name>", '<output-name>.grib')

    with open(filename, 'wb') as out:
        shutil.copyfileobj(datareader.read_all(), out)



List
****

**List data present at the underlying fdb archive and which can be retrieved.**

.. code-block:: python

    fdb_config = pyfdb.Config(config_file.read())
    pyfdb = pyfdb.PyFDB(fdb_config)

    request = pyfdb.FDBToolRequest(
        {
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
        },
    )

    list_iterator = pyfdb.list(request, level=1)
    elements = list(list_iterator)

    for el in elements:
        print(el)

    assert len(elements) == 1

The code above shows an example of listing the contents of the ``FDB`` for a given ``FDBToolRequest``.
The ``FDBToolRequest`` is class describing a ``MarsSelection`` (A MARS request without the verb).

.. note::

   A ``MarsSelection`` doesn't need to be fully specified. In the example above you can see that
   the param key contains a list of values. Contrary to an Identifier, which is a fully specified 
   ``MarsSelection`` and therefore only contains singular values, even ``x/to/y/by/z`` ranges are allowed.


Depending on the given ``level`` different outputs are to be expected:

::

    >>> list_iterator = pyfdb.list(request) // level == 3
    >>> elements = list(list_iterator)
    >>> print(elements[0])

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

    >>> list_iterator = pyfdb.list(request, level=2)
    >>> elements = list(list_iterator)
    >>> print(elements[0])

    {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g}
    {type=an,levtype=sfc},
    length=0,
    timestamp=0

    >>> list_iterator = pyfdb.list(request, level=1)
    >>> elements = list(list_iterator)
    >>> print(elements[0])

    {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g},
    length=0,
    timestamp=0

For each level the returned iterator of ``ListElement`` is restricting the elements to the corresponding
level of the underlying FDB. If you specify ``level=3``, the returned ``ListElement`` contains a valid
``DataHandle`` object. You can use this directly to read the message represented by the ``ListElement``, e.g.:

.. code-block:: python

    list_iterator = pyfdb.list(request, level=3)
    elements = list(list_iterator)

    for el in elements:
        data_handle = el.data_handle()
        data_handle.open()
        assert data_handle.read(4) == b"GRIB"
        data_handle.close()


Inspect
*******

**Inspects the content of the underlying FDB and returns a generator of list elements
describing which field was part of the MARS selection.**

.. code-block:: python

    fdb_config = Config(config_file.read())
    pyfdb = PyFDB(fdb_config)

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

    inspect_iterator = pyfdb.inspect(identifier)
    elements = list(inspect_iterator)

    # Because the request needs to be fully specified, there
    # should be only a single request returned
    assert len(elements) == 1

    for el in elements:
        data_handle = el.data_handle()
        data_handle.open()
        data_handle.read(4) == b"GRIB"
        data_handle.close()

The code above shows how to inspect certain elements stored in the ``FDB``. This call is similar to
a ``list`` call with ``level=3``, although the internals are quite different. The functionality is
designed to list a vast amount of individual fields. 

Similar to the ``list`` command, each ``ListElement`` returned, contains a ``DataHandle`` which can
be used to directly access the data associated with the element, see the example of ``list``.

.. note::

   Due to the internals of the ``FDB`` only a fully specified MARS selection (also called Identifier) is accepted.
   If a range for a key is given, e.g. ``param=131/132``, the second value is silently dropped.


Status
*******

**List the status of all FDB entries with their control identifiers, e.g., whether a certain database was locked for retrieval.**

.. code-block:: python

    fdb_config = Config(config_file.read())
    pyfdb = PyFDB(fdb_config)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
    }

    status_iterator = pyfdb.status(FDBToolRequest(selection))
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
   Use the ``control`` functionality of PyFDB to switch certain properties of ``FDB`` elements.
   Refer to the :ref:`control_label` section for further information.

Wipe
*******

**Wipe data from the database**

Delete FDB databases and the data therein contained. Use the passed
request to identify the database to delete. This is equivalent to a UNIX rm command.
This function deletes either whole databases, or whole indexes within databases

.. tip::

   It's always advised to check the elements of a deletion before running it with the ``doit`` flag.
   Double check that the dry-run, which is active per default, really returns the elements you are
   expecting.

A potential deletion operation could look like this:

.. code-block:: python

    pyfdb = PyFDB(fdb_config_path)

    elements = list(pyfdb.wipe(FDBToolRequest(key_values={"class": "ea"})))
    len(elements) > 0

    # NOTE: Double check that the returned elements are those you want to delete
    for element in elements:
        print(element)
    

    # Do the actual deletion with the `doit=True` flag
    wipe_iterator = pyfdb.wipe(FDBToolRequest(key_values={"class": "ea"}), doit=True)
    wiped_elements = list(wipe_iterator)

    for element in elements:
        print(element)


Move
*******

**Move content of one FDB database to a new URI.**

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

    pyfdb = PyFDB(updated_config)

    # Request for the second level of the schema
    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    move_iterator = pyfdb.move(
        FDBToolRequest(selection),
        URI.from_str(str(new_root)),
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

The example above shows how a database in a pre-existing ``FDB`` can be moved. In this case a
pre-existing ``FDB`` configuration file is loaded and an additional root path is added. It's important
to mention that the move command operates on root folders of ``FDB`` instances.

After specifying the selection we want to move, this has to be an selection which contains keys of 
the first and second level of the schema, we can call the ``move`` function. The returned elements
can be iterated and the actual move operation can be triggered by calling ``execute`` on each element.

Purge
*******
**Remove duplicate data from the database.**

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

    pyfdb = PyFDB(fdb_config_path)

    elements = list(pyfdb.purge(FDBToolRequest(key_values={"class": "ea"})))
    len(elements) > 0

    # NOTE: Double check that the returned elements are those you want to delete
    for element in elements:
        print(element)

    # Do the actual deletion with the `doit=True` flag
    purge_iterator = pyfdb.purge(FDBToolRequest(key_values={"class": "ea"}), doit=True)
    purge_elements = list(purge_iterator)

    for element in purge_elements:
        print(element)

Stats
*******
**Print information about FDB databases, aggregating the information over all the databases visited into a final summary.**

.. code-block:: python

    pyfdb = PyFDB(fdb_config_file)

    request = FDBToolRequest(
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
    )

    elements = list(pyfdb.stats(request))

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
Control
*******
**Enable certain features of FDB databases, e.g., disables or enables retrieving, list, etc.**

.. tip::
   Consume the iterator, returned by the ``control`` call, completely. Otherwise, the lock file
   won't be created.

.. code-block:: python

    pyfdb = PyFDB(fdb_config_file)

    ## Setup of the FDB with the corresponding data
    request = FDBToolRequest(
        {
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "time": "1800",
        },
    )

    print("Lock the database for wiping")
    control_iterator = pyfdb.control(
        request, ControlAction.DISABLE, [ControlIdentifier.WIPE]
    )

    elements = list(control_iterator)
    assert len(elements) == 1

    assert (
        fdb_config_path.parent
        / "db_store"
        / "ea:0001:oper:20200101:1800:g"
        / "wipe.lock"
    ).exists()

    print("Try Wipe")
    wipe_iterator = pyfdb.wipe(request, doit=True)

    elements = list(wipe_iterator)
    assert len(elements) == 0

    print("Unlock the database for wiping")
    control_iterator = pyfdb.control(
        request, ControlAction.ENABLE, [ControlIdentifier.WIPE]
    )

    elements = list(control_iterator)
    assert len(elements) > 0

    assert not (
        fdb_config_path.parent
        / "db_store"
        / "ea:0001:oper:20200101:1800:g"
        / "wipe.lock"
    ).exists()

    print("Try Wipe")
    pyfdb.wipe(request, doit=True)
    pyfdb.flush()

    print("Success")

The example given above shows how the activation/deactivation of the wipe functionality of the ``FDB``
works for a certain selection. 

After specifying the selection we want to target, this has to be a selection which contains keys of 
the first and second level of the schema, we can call the ``control`` function and specify the wished action:
in this case ``ControlIdentifier.WIPE`` and ``ControlAction.DISABLE``, which translate to wanting to disable
wiping for the specified database. We could specify multiple of the ``ControlIdentifier`` in a single call.

For each of the ``ControlIdentifier`` the underlying ``FDB`` will create a ``<control-identifier-name>.lock`` file, 
which resides inside the database specified by the ``FDBToolRequest``. If we decide to enable the action again, this
file gets deleted.

After disabling the action, a call to it results in an empty iterator being returned.

Axes
*******
**Return the 'axes' and their extent of an FDBToolRequest for a given level of the schema in an IndexAxis object.**

If a key isn't specified the entire extent (all values) are returned.

.. code-block:: python

    pyfdb = PyFDB(fdb_config_file)

    request = FDBToolRequest(
        {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            # "date": "20200101", # Left out to show all values are returned
            "levtype": "sfc",
            "step": "0",
            "time": "1800",
        },
    )


    print("---------- Level 3: ----------")
    index_axis: IndexAxis = pyfdb.axes(request)
    # len(index_axis.items()) == 11

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    print("---------- Level 2: ----------")
    index_axis: IndexAxis = pyfdb.axes(request, level=2)
    #len(index_axis.items()) == 8

    for k, v in index_axis.items():
        print(f"k={k} | v={v}")

    print("---------- Level 1: ----------")
    index_axis: IndexAxis = pyfdb.axes(request, level=1)
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

    pyfdb = PyFDB(fdb_config_file)

    request = FDBToolRequest(
        {
            # "type": "an",
            # "class": "ea",
            # "domain": "g",
            # "expver": "0001",
            # "stream": "oper",
            # "date": "20200101",
            # "levtype": "sfc",
            # "step": "0",
            # "time": "180",
        },
        all=True,
    )

    index_axis: IndexAxis = pyfdb.axes(request)

Enabled
*******
**Check whether a specific control identifier is enabled.**

.. code-block:: python

    pyfdb = PyFDB(fdb_config_file)

    assert pyfdb.enabled(ControlIdentifier.NONE) is True
    assert pyfdb.enabled(ControlIdentifier.LIST) is True
    assert pyfdb.enabled(ControlIdentifier.RETRIEVE) is True
    assert pyfdb.enabled(ControlIdentifier.ARCHIVE) is True
    assert pyfdb.enabled(ControlIdentifier.WIPE) is True
    assert pyfdb.enabled(ControlIdentifier.UNIQUEROOT) is True

The examples above show how a default ``FDB`` is configured, this is, all possible ``ControlAction`` s
are enabled by default.

Configuring the ``FDB`` to disallow writing via setting ``writable = False`` in the ``fdb_config.yaml``,
we end up with the following ``ControlIdentifier`` s:

.. code-block:: python

    fdb_config = yaml.safe_load(fdb_config_path.read_text())
    fdb_config["writable"] = False

    pyfdb = PyFDB(fdb_config)

    assert pyfdb.enabled(ControlIdentifier.NONE) is True
    assert pyfdb.enabled(ControlIdentifier.LIST) is True
    assert pyfdb.enabled(ControlIdentifier.RETRIEVE) is True
    assert pyfdb.enabled(ControlIdentifier.ARCHIVE) is False
    assert pyfdb.enabled(ControlIdentifier.WIPE) is False
    assert pyfdb.enabled(ControlIdentifier.UNIQUEROOT) is True

The configuration changes accordingly, if we substitute ``writable = False`` with ``visitable = False``.

Needs Flush
*************
**Return whether a flush of the FDB is needed.**

.. code-block:: python

    pyfdb = PyFDB(fdb_config_file)

    filename = test_data_path / "x138-300.grib"

    pyfdb.archive(open(filename, "rb").read())
    pyfdb.needs_flush() # == True
    pyfdb.flush()
    pyfdb.needs_flush() # == False

The example above shows return value of the ``flush`` command after an archive command results in ``True``. 
Flushing resets the internal status of the ``FDB`` and the call to ``needs_flush`` returns ``False`` afterwards.
