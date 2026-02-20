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

This variable takes precedence over both ``FDB_CONFIG_FILE`` and the default config file (``$FDB_HOME/etc/fdb/config.yaml`` or similar, see below).


``FDB_CONFIG_FILE``
--------------------

If set, FDB parses the value of ``FDB_CONFIG_FILE`` as a path to a YAML configuration file.

.. code-block:: console

   export FDB_CONFIG_FILE=/path/to/config.yaml

This takes precedence over the default config file (see below), but is
overridden by ``FDB_CONFIG``.


Default Config File Location
------------------------------

If neither ``FDB_CONFIG`` nor ``FDB_CONFIG_FILE`` are set, FDB will search for a configuration file
in ``$FDB_HOME/etc/fdb/`` using the following filenames (in order):

1. ``<app-display-name>.yaml`` or ``<app-display-name>.json``
2. ``<app-name>.yaml`` or ``<app-name>.json``
3. ``config.yaml`` or ``config.json``

where ``<app-display-name>`` and ``<app-name>`` are determined by the FDB application.