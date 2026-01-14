fdb dump
========

Dump the structural contents of the FDB. In particular, in the TOC formulation, enumerate the different entries in the Table of Contents (including INIT and CLEAR entries).

The dump will include information identifying the data files that are referenced, and the "Axes" which describe the maximum possible extent of the data that is contained in the database.

Usage
-----

``fdb dump [options] [request1] [request2] ...``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--simple``                           | Also print the location of each field                                                                               |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--ignore-errors``                    | Ignore errors (report them as warnings) and continue processing wherever possible                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--raw``                              | Don't apply (contextual) expansion and checking on requests                                                         |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | Visit all FDB databases (for testing)                                                                               |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename.                                                                                         |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+

Examples
--------

Example 1
---------
Pass a partial request (as a key), to dump the contents of all databases in the FDB that match that key.
::

  % fdb dump class=od,stream=oper

  TOC_INIT  2017-01-05 10:16:44.036480, version:1, fdb: 70700, uid: <id>, pid 1367 , host: glados
    Key: {class=od,expver=0001,stream=oper,date=20160907,time=0000,domain=g}

  TOC_INDEX 2017-01-05 10:16:44.086367, version:1, fdb: 70700, uid: <id>, pid 1367 , host: glados
    Path: an:pl.20170105.101644.glados.5871220293633.index, offset: 0, type: BTreeIndex
    Prefix: an:pl, key: {type=an,levtype=pl}
    Files:
      0   => /data/fdb/od:0001:oper:20160907:0000:g/an:pl.20170105.101644.glados.5871220293634.data
      Axes:
        levelist
          1000
          300
          400
          500
          700
          850
          param
          130
          step
          0

  Dumping <path>testcases/fdb5/fdb_root/root/od:0001:oper:20160907:1200:g


Example 2
---------
The --simple option provides a more concise output.
::

  % fdb dump --simple class=od,expver=0001
  TOC_INIT  2017-01-05 10:16:44.059914, version:1, fdb: 70700, uid: <id>, pid 1367 , host: glados  Key: {class=od,expver=0001,stream=oper,date=20160907,time=1200,domain=g}
  TOC_INDEX 2017-01-05 10:16:44.095547, version:1, fdb: 70700, uid: <id>, pid 1367 , host: glados  Path: an:pl.20170105.101644.glados.5871220293636.index, offset: 0, type: BTreeIndex  Prefix: an:pl, key: {type=an,levtype=pl}


Example 3
---------
The --all option visits all available entries
::
  
  % fdb dump --simple --all
  TOC_INIT  2017-01-05 10:16:44.059914, version:1, fdb: 70700, uid: <id>, pid 1367 , host: glados  Key: {class=od,expver=0001,stream=oper,date=20160907,time=1200,domain=g}
  TOC_INDEX 2017-01-05 10:16:44.095547, version:1, fdb: 70700, uid: <id>, pid 1367 , host: glados  Path: an:pl.20170105.101644.glados.5871220293636.index, offset: 0, type: BTreeIndex  Prefix: an:pl, key: {type=an,levtype=pl}

