fdb where
=========

Print the location of FDB5 database.

The output format of this tool is stable when used with --porcelain.

Usage
-----

``fdb where [options] [request1] [request2] ...``


Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--porcelain``                        | Streamlined and stable output for input into other tools                                                            |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--ignore-errors``                    | Ignore errors (report them as warnings) and continue processing wherever possible                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--raw``                              | | Don't apply (contextual) expansion and checking on requests.                                                      |
|                                        | | Keys and values passed must match those used internally to the FDB exactly.                                       |
|                                        | | This prevents the use of named parameters (such as t rather than param=130), dates (such as date=-1), or similar. |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--minimum-keys``                     | | Default is class,expver                                                                                           |
|                                        | | Define the minimum set of keys that must be specified. This prevents inadvertently exploring the entire FDB.      |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | (Debug and testing only) Visit all FDB databases                                                                    |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename                                                                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+


Example
-------

Given a MARS request, find the location of the given FDB database.
::
  
  % fdb where class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g
  /data/fdb/od:0001:oper:20151004:1200:g

