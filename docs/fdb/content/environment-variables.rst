Environment variables
=====================

Here we document the environment variables that affect the FDB configuration.

``FDB_CONFIG``
---------------

If set, FDB parses the value of ``FDB_CONFIG`` as a YAML configuration *string* (i.e. the
variable contains YAML, not a path). For example:

.. code-block:: console

   export FDB_CONFIG='{
     schema: /path/to/schema,
     spaces: [{
       roots: [{path: /path/to/root}]
     }]
   }'

This variable takes precedence over both ``FDB_CONFIG_FILE`` and the default config file (``$FDB_HOME/etc/fdb/config.yaml``).


``FDB5_CONFIG``
---------------
Deprecated. Equivalent to ``FDB_CONFIG``. If both are specified, ``FDB_CONFIG`` takes precedence over ``FDB5_CONFIG``.


``FDB_SUB_TOCS``
------------------

If set to `1`, the FDB process will write to its own sub toc files instead of the main toc file.
This mode is useful for avoiding contention on the main toc file when multiple FDB processes are writing concurrently.

This variable overrides the `useSubToc` flag provided by the user config.


``FDB_CONFIG_FILE``
--------------------

If set, FDB parses the value of ``FDB_CONFIG_FILE`` as a path to a YAML configuration file.

.. code-block:: console

   export FDB_CONFIG_FILE=/path/to/config.yaml

This takes precedence over the default config file (``$FDB_HOME/etc/fdb/config.yaml``), but is
overridden by ``FDB_CONFIG``.


``FDB5_CONFIG_FILE``
--------------------
Deprecated. Equivalent to ``FDB_CONFIG_FILE``. If both are specified, ``FDB_CONFIG_FILE`` takes precedence over ``FDB5_CONFIG_FILE``.


.. note::

   Several FDB environment variables historically used an ``FDB5_`` prefix. These are being
   deprecated in favour of an ``FDB_`` prefix. Where both forms are accepted, the ``FDB_`` form
   takes precedence and the ``FDB5_`` form is retained only for backwards compatibility.


Configuration file locations
----------------------------

The following variables override the paths of the individual configuration files that FDB reads.
Each value may use ``~fdb`` (expanded from ``FDB_HOME`` or the ``fdb_home`` config value) and other
tilde-prefixed home shortcuts.

``FDB_SCHEMA_FILE``
-------------------

Path to the FDB schema file, which defines the set of keys and their order used to index data.

Default: ``~fdb/etc/fdb/schema``.

If a ``schema`` is set in the FDB configuration, that value takes precedence over this variable.


``FDB_ROOTS_FILE``
------------------

Path to the file listing the FDB root directories (the file spaces available for storing data).

Default: ``~fdb/etc/fdb/roots``.


``FDB_SPACES_FILE``
-------------------

Path to the file describing the FDB file spaces (how roots are grouped and which handler assigns
data to them).

Default: ``~fdb/etc/fdb/spaces``.


``FDB_DBNAMES_FILE``
--------------------

Path to the file describing the database path namers, used to customise how database directory
names are generated from keys.

Default: ``~fdb/etc/fdb/dbnames``.


``FDB_ENGINES_FILE``
--------------------

Path to the file mapping databases to the engine (backend) that should serve them.

Default: ``~fdb/etc/fdb/engines``.


``FDB_EXPVER_FILE``
-------------------

Path to the file used by the ``expver`` file space handler to map an ``expver`` to the FDB root in
which its data is stored.

Default: ``~fdb/etc/fdb/expver_to_fdb_root.map``.


Root and file-space selection
-----------------------------

``FDB_ROOT_DIRECTORY``
----------------------

If set to a non-empty path, FDB bypasses the configured file spaces and uses this single directory
as the only root, enabling it for listing, retrieval, archival and wiping.

Default: unset (file spaces from configuration are used).


``FDB_ROOT``
------------

Used by the ``expver`` file space handler to force new ``expver`` data to be written to a specific
root. The value must be one of the roots that support archival, otherwise an error is raised. If the
``expver`` is already mapped to a different root, the existing mapping is kept and a warning is
emitted.

Default: unset.


``FDB5_ROOT``
-------------
Deprecated. Equivalent to ``FDB_ROOT``. If both are specified, ``FDB_ROOT`` takes precedence over ``FDB5_ROOT``.


``FDB_FILESPACEHANDLER_ENVVARNAME``
-----------------------------------

Name of the environment variable that the ``envvar`` file space handler consults to select the
filesystem to write to. The referenced variable must contain a path to an existing directory.

Default: ``STHOST`` (i.e. the ``envvar`` handler reads the ``$STHOST`` variable by default).


Write behaviour
---------------

``FDB_ASYNC_WRITE``
-------------------

If set to a true value, data is written using an asynchronous IO handle instead of a synchronous
file handle.

Default: ``false``.


``FDB_WRITE_TO_NULL``
---------------------

If set to a true value, data writes are directed to an empty (null) handle and discarded. This is
intended for benchmarking and testing the write path without producing output.

Default: ``false``.


``FDB_DATA_SYNC_ON_FLUSH``
--------------------------

If set to a true value, ``fdatasync`` is called when a data file is flushed, ensuring the data is
committed to disk. Set to a false value to skip the sync (faster, but less durable).

Default: ``true``.


``FDB_DATA_WRITE_QUEUE_LENGTH``
-------------------------------

Maximum length of the queue of pending data-write requests used by the remote client connection.

Default: ``320``.


``FDB_DEDUPLICATE_FIELDS``
--------------------------

If set to a true value, fields retrieved for a request are deduplicated: the retrieved fields are
arranged into a hypercube and only the last field matching each unique combination of keys is
returned.

Default: ``false``.


Read behaviour
--------------

``FDB_CACHE_TOCS_ON_READ``
--------------------------

If set to a true value, table-of-contents (TOC) files are cached in memory when they are opened for
reading, avoiding repeated reads of the same TOC.

Default: ``true``.


``FDB_OPEN_NOATIME``
--------------------

If set to a true value, TOC files are opened with the ``O_NOATIME`` flag so that reading them does
not update their access time. This may require appropriate filesystem permissions and has no effect
on platforms without ``O_NOATIME``.

Default: ``false``.


``FDB_SEEKABLE_DATA_HANDLE``
----------------------------

If set to a true value, ``retrieve`` returns a seekable data handle (a ``FieldHandle``) rather than
a plain streamed handle.

Default: ``false``.


``FDB_READ_LIMIT``
------------------

Memory limit, in bytes, for the remote read limiter, which caps the amount of data buffered in
memory while reading from a remote FDB.

Default: the ``limits.read`` value from the user configuration, or 1 GiB if unset.


``FDB_LOAD_INDEX_THREADS``
--------------------------

Number of threads used to construct index objects when loading a TOC.

Default: ``1``.


``FDB_SEARCH_CASESENSITIVE_DB``
-------------------------------

If set to a true value, the search for a database directory on disk is case-sensitive.

Default: ``true``.


Lustre striping
---------------

These variables only take effect when FDB is built with Lustre support.

``FDB_HANDLE_LUSTRE_STRIPE``
----------------------------

If set to a true value (and Lustre support is compiled in), FDB applies the configured Lustre stripe
settings to the files it creates.

Default: ``true``.


``FDB_DATA_LUSTRE_STRIPE_COUNT``
--------------------------------

Lustre stripe count applied to data files.

Default: ``8``.


``FDB_DATA_LUSTRE_STRIPE_SIZE``
-------------------------------

Lustre stripe size, in bytes, applied to data files.

Default: ``8 MiB`` (8388608).


``FDB_INDEX_LUSTRE_STRIPE_COUNT``
---------------------------------

Lustre stripe count applied to index files.

Default: ``1``.


``FDB_INDEX_LUSTRE_STRIPE_SIZE``
--------------------------------

Lustre stripe size, in bytes, applied to index files.

Default: ``8 MiB`` (8388608).


Indexing
--------

``FDB_INDEX_TYPE``
------------------

Overrides the index type used for new indexes. When left empty, FDB selects ``BTreeIndex`` for keys
shorter than 8 characters and ``BTreeIndex64`` for longer keys.

Default: unset (automatic selection).


Auxiliary data
--------------

``FDB_AUX_EXTENSIONS``
----------------------

Set of filename extensions for auxiliary files associated with each archived field. For every data
file written, FDB also considers auxiliary files sharing the field's path with one of these
extensions appended.

Default: ``gribjump``.


Protocol and serialisation versions
------------------------------------

``FDB_REMOTE_PROTOCOL_VERSION``
-------------------------------

Overrides the protocol version used to communicate with a remote FDB. A value of ``0`` means the
built-in default version is used. An unsupported value causes an error.

Default: ``0``.


``FDB5_REMOTE_PROTOCOL_VERSION``
--------------------------------
Deprecated. Equivalent to ``FDB_REMOTE_PROTOCOL_VERSION``. If both are specified, ``FDB_REMOTE_PROTOCOL_VERSION`` takes precedence over ``FDB5_REMOTE_PROTOCOL_VERSION``.


``FDB_SERIALISATION_VERSION``
-----------------------------

Overrides the TOC serialisation version used when writing. A value of ``0`` means the default
version is used. An unsupported value causes an error.

Default: ``0``.


``FDB5_SERIALISATION_VERSION``
------------------------------
Deprecated. Equivalent to ``FDB_SERIALISATION_VERSION``. If both are specified, ``FDB_SERIALISATION_VERSION`` takes precedence over ``FDB5_SERIALISATION_VERSION``.