fdb move
========

Move the content of one FDB database. This locks the source database, make it possible to create a second database in another root, duplicates all data. Source data are not automatically removed.

This tool is envisaged for migrating data from two different data store (i.e. from a performance to a capacity data store)

Usage
-----

``fdb move [options] [request1] [request2] ...``

Options
-------

Database request must match exactly one database.

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--dest``                             | | The absolute or relative path of a FDB root that can contain a copy of the source database                        |
|                                        | | (wipe has to be enabled, archival can be disabled)                                                                |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--keep``                             | Keep the source DB                                                                                                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--delay``                            | Delay in sec. before start deleting the source DB. The default is 0 sec. (ignored in case of --keep)                |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--threads``                          | Number of concurrent threads moving data to dest DB                                                                 |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--minimum-keys=string,string,...``   | Use these keywords as a minimum set which *must* be specified                                                       |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--raw``                              | Don't apply (contextual) expansion and checking on requests                                                         |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--ignore-errors``                    | Ignore errors (report them as warnings) and continue processing wherever possible                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | Visit all FDB databases                                                                                             |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename                                                                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+


Example
-------

This tool is non-destructive (it only copies data), but it affects the status of the source DB by applying locks.
::
  % fdb move class=rd,expver=xxxx,stream=oper,date=20160907,time=0000,domain=g --dest=../fdb/root2 --delay=10 --threads=16