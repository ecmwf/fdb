grib2fdb5
=========

Inserts data into the FDB, creating a new databases if needed.

Note that the interface is designed to be the same as the older grib2fdb tool for use with fdb4.

Usage
-----

``grib2fdb5 [-c class] [-e expver] [-T type] [-s stream] [-f file]``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--c``                                | Check that the class of the supplied data matches                                                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--e``                                | Check that the expver of the supplied data matches                                                                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--T``                                | Check that the type of the supplied data matches                                                                    |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--s``                                | Check that the stream of the supplied data matches                                                                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--f``                                | The grib file to archive                                                                                            |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename                                                                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+



Example 1
---------

You may pass multiple grib files. The tool will insert them sequentially.
::  

  % grib2fdb5 -f data.grib
  Processing data.grib
  Key {}
  FDB archive 12 fields, size 37.5412 Mbytes, in 0.08579 second (437.584 Mbytes per second)
  fdb::service::archive: 0.085856 second elapsed, 0.085854 second cpu

Example 2
---------

Check that the supplied keys match
::
  
  % grib2fdb5 -c rd -e xxxx -T an -s oper -f data.grib
  Processing data.grib
  Key {class=rd,expver=xxxx,type=an,stream=oper}
  FDB archive 12 fields, size 37.5412 Mbytes, in 0.086995 second (431.518 Mbytes per second)
  fdb::service::archive: 0.087076 second elapsed, 0.087075 second cpu