.. _FDB_AppConfig:

Application config
==================

Besides the :doc:`FDB configuration <config-schema>` (the *system config*, which describes *where*
and *how* data is stored), an FDB instance can be given a second, optional configuration: the
*application config*. It carries tuning options that belong to the process using the FDB rather
than to the FDB installation: for example whether this process writes sub-TOCs, or how many threads
it may use to read.

Keeping the two apart means that a single, shared system config can be used by many applications,
each of which adjusts its own behaviour without editing that file.

.. note::

   The application config was previously called the *user config*. The old names
   (``user_config`` in PyFDB, ``UserConfig`` in the Rust crate, ``Config::userConfig()`` in C++)
   still work but are deprecated; see :ref:`FDB_AppConfig_Migration`.


Available options
-----------------

The application config is a YAML (or equivalent key/value) document. Unknown keys are ignored, and
every key is optional.

.. list-table::
   :header-rows: 1
   :widths: 22 12 18 48

   * - Key
     - Type
     - Default
     - Description
   * - ``useSubToc``
     - bool
     - ``false``
     - Write to a per-process sub-TOC instead of the main TOC of each database. This avoids
       contention on the main TOC when many processes write concurrently. Overridden (forced on) by
       the ``FDB_SUB_TOCS`` environment variable.
   * - ``preloadTocBTree``
     - bool
     - ``true``
     - When a database is opened for reading, load the ``BTree`` of each index into memory up
       front, which speeds up subsequent lookups. Set to ``false`` to trade lookup speed for a
       lower memory footprint. No environment variable.
   * - ``readIndexThreads``
     - integer
     - ``1``
     - Number of threads used, per database, for read-side work that can be split across indexes.
       Clamped to the range 1-64. Overridden by ``FDB_READ_INDEX_THREADS``.
   * - ``apiQueueSize``
     - integer
     - ``100``
     - Capacity of the queues that carry results from the worker threads to the caller. Clamped to
       the range 1-1048576. Overridden by ``FDB_API_QUEUE_SIZE``.
   * - ``apiMaxQueues``
     - integer
     - ``2 * readIndexThreads``
     - Maximum number of sub-queues open at once when results are gathered per index. Clamped to the
       range 1-1024. Overridden by ``FDB_API_MAX_QUEUES``.

A minimal example:

.. code-block:: yaml

   useSubToc: true
   readIndexThreads: 8


Precedence
----------

For every option that has an environment variable (see :doc:`environment-variables`), the
precedence is, from highest to lowest:

1. the environment variable (or the corresponding ``eckit`` resource, e.g. ``fdbReadIndexThreads``);
2. the value in the application config;
3. the built-in default listed above.

The application config stays attached to the FDB instance however the system config was located:
from a file given on the command line, from ``FDB_CONFIG`` / ``FDB_CONFIG_FILE``, or from the
default location under ``$FDB_HOME``. It is also passed on to every lane of a ``select`` FDB and to
the inner FDB of a remote server connection.


Supplying an application config
-------------------------------

PyFDB
^^^^^

Pass ``app_config`` as the second argument of the ``FDB`` constructor. Like ``config``, it can be a
``str`` (YAML), a ``dict`` or a ``Path``:

.. code-block:: python

   fdb = pyfdb.FDB(config, app_config={"useSubToc": True})

The two configurations in use are returned by ``FDB.config()``:

.. code-block:: python

   system_config, app_config = fdb.config()

C API
^^^^^

Pass the application config as a YAML string to ``fdb_new_handle_from_yaml``:

.. code-block:: c

   fdb_handle_t* fdb;
   fdb_new_handle_from_yaml(&fdb, system_config_yaml, "useSubToc: true");

C++
^^^

The application config is the second argument of the ``fdb5::Config`` constructor, of
``fdb5::Config::make`` and of ``fdb5::LibFdb5::defaultConfig``. It is read back with
``Config::appConfig()``:

.. code-block:: cpp

   eckit::LocalConfiguration appConfig;
   appConfig.set("useSubToc", true);

   fdb5::FDB fdb(fdb5::Config::make("/path/to/config.yaml", appConfig));

Rust
^^^^

Use the ``AppConfig`` struct as the second argument of ``Fdb::open``. A main config must be given
as well:

.. code-block:: rust

   let fdb = Fdb::open(
       Some(&cfg),
       Some(AppConfig { use_sub_toc: true, ..Default::default() }),
   )?;

``AppConfig`` exposes ``use_sub_toc`` and ``preload_toc_btree``.

Command-line tools
^^^^^^^^^^^^^^^^^^

The command-line tools build an application config themselves from their options: ``--threads``
of :doc:`fdb list <../cli_tools/list>`, :doc:`fdb purge <../cli_tools/purge>` and ``fdb stats``
sets ``readIndexThreads``, and ``fdb-hammer`` sets ``useSubToc`` (use ``--disable-subtocs`` to
turn it off).


.. _FDB_AppConfig_Migration:

Migrating from "user config"
----------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - API
     - Deprecated
     - Use instead
   * - PyFDB
     - ``FDB(config, user_config=...)`` (keyword-only; emits a ``DeprecationWarning``)
     - ``FDB(config, app_config=...)``. Passing both raises a ``ValueError``.
   * - C++
     - ``Config::userConfig()``
     - ``Config::appConfig()``
   * - Rust
     - ``UserConfig``
     - ``AppConfig``
   * - C API
     - parameter ``user_config`` of ``fdb_new_handle_from_yaml``
     - ``app_config`` (same position; no source change needed)

Positional callers, such as ``pyfdb.FDB(config, {...})``, keep working unchanged.
