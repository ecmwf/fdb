fdb purge
=========

Purge duplicate entries from the database and remove the associated data (if the data is owned, not adopted).

Data in the FDB5 is immutable. It is masked, but not damaged or deleted, when it is overwritten with new data using the same key. If an index or data file only contains masked data (i.e. no data that can be obtained using normal requests), then those indexes and data files may be removed.

If an index refers to data that is not owned by the FDB (in particular data which has been adopted from an existing FDB4), this data will not be removed.

Usage
-----

``fdb purge [options] [request1] [request2] ...``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--doit``                             | Delete the files (data and indexes)                                                                                 |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--porcelain``                        | Print only a list of files to be deleted / that are deleted                                                         |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--ignore-errors``                    | Ignore errors (report them as warnings) and continue processing wherever possible.                                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--ignore-no-data``                   | Do not return error if there is no data to delete.                                                                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--raw``                              | | Don't apply (contextual) expansion and checking on requests.                                                      |
|                                        | | Keys and values passed must match those used internally to the FDB exactly.                                       |
|                                        | | This prevents the use of named parameters (such as t rather than param=130), dates (such as date=-1), or similar. |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--minimum-keys=string,string``       | | Default is class,expver,stream,date,time                                                                          |
|                                        | | Define the minimum set of keys that must be specified. This is a safety precaution against accidental data removal|
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | (Debug and testing only) Visit all FDB databases                                                                    |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename                                                                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+

Example 1
---------

Pass the location of a database in to report on the extent of duplication of fields.

If required, the location can be obtained using the fdb-root command.
::

  % du -sh /data/fdb/od\:0001\:oper\:20160907\:1200\:g/
  209M    /data/fdb/od:0001:oper:20160907:1200:g/

  % fdb purge class=od,expver=0001,stream=oper,date=20160907,time=1200

  Purging for request
  retrieve,
    class=od,
    expver=0001,
    stream=oper,
    date=20160907,
    time=1200

  Index Report:
  ...
    Index TocIndex[path=<path>/testcases/fdb5/fdb_root/root/od:0001:oper:20160907:1200:g/an:pl.20161208.150123.glados.78181289689092.index,offset=0]
      Fields                          : 6
      Size of fields                  : 19,682,388 (18.7706 Mbytes)
      Duplicated fields               : 6
      Size of duplicates              : 19,682,388 (18.7706 Mbytes)
  ...
  Rerun command with --doit flag to delete unused files

  % du -sh /data/fdb/od\:0001\:oper\:20160907\:1200\:g/
  209M    /data/fdb/od:0001:oper:20160907:1200:g/

Example 2
---------

Additionally pass the --doit flag to delete the duplicates.
::  
  
  % du -sh /data/fdb/od\:0001\:oper\:20160907\:1200\:g/
  209M    /data/fdb/od:0001:oper:20160907:1200:g/

  % fdb purge --doit class=od,expver=0001,stream=oper,date=20160907,time=1200

  Purging for request
  retrieve,
    class=od,
    expver=0001,
    stream=oper,
    date=20160907,
    time=1200

  Index Report:
  ...
    Index TocIndex[path=<path>/testcases/fdb5/fdb_root/root/od:0001:oper:20160907:1200:g/an:pl.20161208.150123.glados.78181289689092.index,offset=0]
      Fields                          : 6
      Size of fields                  : 19,682,388 (18.7706 Mbytes)
      Duplicated fields               : 6
      Size of duplicates              : 19,682,388 (18.7706 Mbytes)
  ...
  
  % du -sh /data/fdb/od\:0001\:oper\:20160907\:1200\:g/
  20M     fdb_root/root/od:0001:oper:20160907:1200:g/