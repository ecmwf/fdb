fdb dump-toc
============

Description
-----------

Dump the structural contents of a particular toc file (or subtoc file). This will dump the actual contents of the entries for debugging purposes.

Usage
-----

``fdb dump-toc [options] [path1] [path2]``

Options
-------

+----------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``--walk``                             | Walk through tocs any linked subtocs in logically correct view. This displays the contents presented by any linked subtocs, and masks any masked entries, to show the contents as would be presented to the user.|
+----------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename.                                                                                                                                                                                      |
+----------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Examples
--------
Example 1
---------

Dump the contents of a normal toc
::

  % fdb dump-toc /data/fdb/rd:xxxx:oper:20160907:0000:g/toc
  TOC_INIT  2019-06-07 14:17:01.032360, version:2, fdb: 50308, uid: <id>, pid 31508, host: <host>  Key: {class=rd,expver=xxxx,stream=oper,date=20160907,time=1200,domain=g}, sub-toc: no
  TOC_SUB_TOC 2019-06-07 14:17:01.818271, version:2, fdb: 50308, uid: <id>, pid 31508, host: <host>  Path: toc.20190607.141701.<host>.135325829562377
  TOC_INDEX 2019-06-07 14:17:01.943474, version:2, fdb: 50308, uid: <id>, pid 31508, host: <host>  Path: an:pl.20190607.141701.<host>.135325829562374.index, offset: 0, type: BTreeIndex  Prefix: an:pl, key: {type=an,levtype=pl}
  TOC_CLEAR 2019-06-07 14:17:01.943563, version:2, fdb: 50308, uid: <id>, pid 31508, host: <host>  Path: toc.20190607.141701.<host>.135325829562377, offset: 0
  ...

Example 2
---------

Dump the contents of a toc walking the subtocs logically. Note that subtocs that are hidden by TOC_CLEAR entries will be hidden in the walking process, along with any masked indexes.
::
  
  % fdb dump-toc --walk /data/fdb/rd:xxxx:oper:20160907:0000:g/toc
  TOC_INIT  2019-06-07 14:17:01.032360, version:2, fdb: 50308, uid: <id>, pid 31508, host: <host>  Key: {class=rd,expver=xxxx,stream=oper,date=20160907,time=1200,domain=g}, sub-toc: no
  TOC_INDEX 2019-06-07 14:28:31.850438, version:2, fdb: 50308, uid: <id>, pid 644  , host: <host>  Path: an:pl.20190607.142831.<host>.2765958938625.index, offset: 0, type: BTreeIndex  Prefix: an:pl, key: {type=an,levtype=pl}
  TOC_INDEX 2019-06-07 14:17:01.943474, version:2, fdb: 50308, uid: <id>, pid 31508, host: <host>  Path: an:pl.20190607.141701.<host>.135325829562374.index, offset: 0, type: BTreeIndex  Prefix: an:pl, key: {type=an,levtype=pl}
  ...