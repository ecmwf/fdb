fdb info
========

Get information about the FDB configuration and binaries

Usage
-----

``fdb info [options]``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | Print all available information                                                                                     |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--version``                          | Print the version of the FDB software                                                                               |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--home``                             | Print the location containing the etc directory searched for the FDB configuration files                            |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--schema``                           | Print the location of the default FDB schema file                                                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config-file``                      | Print the location of the FDB configuration file if being used                                                      |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename                                                                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--lustre-api``                       | Indicate if the Lustre API is supported or disabled in this build                                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+

Example
-------

Get all information snippets
::

  % fdb info --all
  Version: 5.4.0
  Home: <path>/fdbprod
  Schema: <path>/fdbprod/etc/fdb/schema
  Config: <path>/testcases/fdb5/fdb5_simple.yaml

Get version information in an easily parsable form, for possible use in shell scripts.
::

  % fdb info --version
  7.8.0

Get location of FDB home configuration in an easily parsable form:
::

  % fdb info --home
  <path>/fdbprod

Get location of FDB schema file in an easily parsable form:
::

  % fdb info --schema
  <path>/fdbprod/etc/fdb/schema

Get location of current FDB configuration file in an easily parsable form:
::
  
  % fdb info --config
  <path>/testcases/fdb5/fdb5_simple.yaml