fdb stats
=========

Prints information about FDB databases, aggregating the information over all the databases visited into a final summary.

Usage
-----

``fdb stats [options] [request1] [request2] ...``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--details``                          | Print information for each database visited, in addition to the summary                                             |
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
| ``--config=string``                    | FDB configuration filename.                                                                                         |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+

Example 1
---------

You may pass a partial request (as a key) that will print information on all FDB databases that match that key.

::

  % fdb stats class=od,expver=0001,stream=oper,date=20151001
  ...
  Scanning /data/mars_p_d17_d17_1_15/fdb/od:0001:oper:20151001:1200:g

  Summary:
  ========

  Database Type 'toc'
  Databases                       : 1
  Fields                          : 1,080
  Size of fields                  : 3,542,829,840 (3.29952 Gbytes)
  Duplicated fields               : 1,056
  Size of duplicates              : 3,464,100,288 (3.22619 Gbytes)
  Reacheable fields               : 24
  Reachable size                  : 78,729,552 (75.0824 Mbytes)
  TOC records                     : 46
  Size of TOC files               : 47,104 (46 Kbytes)
  Size of schemas files           : 15,413 (15.0518 Kbytes)
  TOC records                     : 46
  Owned data files                : 45
  Size of owned data files        : 3,542,829,840 (3.29952 Gbytes)
  Index files                     : 45
  Size of index files             : 5,898,240 (5.625 Mbytes)
  Size of TOC files               : 47,104 (46 Kbytes)
  Total owned size                : 3,548,790,597 (3.30507 Gbytes)
  Total size                      : 3,548,790,597 (3.30507 Gbytes)

Example 2
---------

The --details flag prints a report per database that is visited, as well as the overall summary

::
  
  % fdb stats class=od,expver=0001
  ...
  Scanning /data/fdb/od:0001:oper:20151003:1200:g

  Fields                          : 6,336
  Size of fields                  : 20,784,601,728 (19.3572 Gbytes)
  Duplicated fields               : 6,240
  Size of duplicates              : 20,469,683,520 (19.0639 Gbytes)
  Reacheable fields               : 96
  Reachable size                  : 314,918,208 (300.329 Mbytes)
  TOC records                     : 268
  Size of TOC files               : 274,432 (268 Kbytes)
  Size of schemas files           : 61,652 (60.207 Kbytes)
  TOC records                     : 268
  Owned data files                : 264
  Size of owned data files        : 20,784,601,728 (19.3572 Gbytes)
  Index files                     : 264
  Size of index files             : 34,603,008 (33 Mbytes)
  Size of TOC files               : 274,432 (268 Kbytes)
  Total owned size                : 20,819,540,820 (19.3897 Gbytes)
  Total size                      : 20,819,540,820 (19.3897 Gbytes)

  Scanning /data/fdb/od:0001:oper:20151002:1200:g
  ...

  Summary:
  ========

  Number of databases             : 4
  ...