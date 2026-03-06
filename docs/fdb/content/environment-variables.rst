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


``FDB_SUB_TOCS``
------------------

If set to `1`, the FDB process will write to its own sub toc files instead of the main toc file.
This mode is useful for avoiding contention on the main toc file when multiple FDB processes are writing concurrently.

This variable overrides the `useSubToc` flag provided by the user config.