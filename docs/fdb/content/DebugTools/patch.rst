fdb patch
=========

Description
-----------

Rearchive the FDB corresponding to a given request into another, whilst modifying the class or expver elements of the database key. Note that this copies all of the data.

Usage
-----
``fdb patch [options] [request1]``

Options
-------

At least one of class or expver is required is required


+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--expver=string``                    | Specifies the new class to use for the copied data                                                                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--class=string``                     | Specifies the new experiment version to use for the copied data                                                     |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--ignore-errors``                    | Ignore errors (report them as warnings) and continue processing wherever possible                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--raw``                              | | Don't apply (contextual) expansion and checking on requests. This prevents the use of named parameters            |
|                                        | | (such as t rather than param=130), dates (such as date=-1), or similar. Keys and values passed must match those   | 
|                                        | | used internally to the FDB exactly.                                                                               |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--minimum-keys``                     | Default is class,expver                                                                                             |
|                                        | Define the minimum set of keys that must be specified. Prevents inadvertently exploring and copying the entire FDB. |                                                                                                 
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | (Debug and testing only) Visit all FDB databases                                                                    |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename.                                                                                         |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+

Example
-------

You may pass a partial request (as a key) that will list all the field in the FDB that match that key.

Note that this is a global search through all the databases of the FDB that match this key.
::
  
  % fdb patch --expver=xxxz class=rd,expver=xxxx,stream=oper,date=20160907,time=1200/0000,domain=g
  Compress handle: 0.0001 second elapsed, 0.0001 second cpu
  FDB archive 12 fields, size 37.5412 Mbytes, in 0.279314 second (134.403 Mbytes per second)
  fdb::service::archive: 0.279343 second elapsed, 0.090249 second cpu
  
  Summary
  =======
  
  12 fields (37.5412 Mbytes) copied to {expver=xxxz}
  Rates: 114.971 Mbytes per second, 36.7503 fields/s
  fdb patch: 0.946881 second elapsed, 0.159061 second cpu