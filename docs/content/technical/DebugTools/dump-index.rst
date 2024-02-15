fdb dump-index
==============

Dump the contents of a particular index file for debugging purposes.

Note that one index file can (and likely will) contain multiple indexes, which will be dumped sequentially.
Usage
-----

``fdb dump-index [options] [path1] [path2]``

Options
-------

Examples
--------

Example 1
---------

Dump the contents of an index file.
::
  
  % fdb dump-index /data/fdb/rd:xxxx:oper:20160907:0000:g/an:pl.20190607.142831.glados.2765958938626.index
  Dumping contents of index file /data/fdb/rd:xxxx:oper:20160907:0000:g/an:pl.20190607.142831.glados.2765958938626.index
    Path: an:pl.20190607.142831.glados.2765958938626.index, offset: 0, type: BTreeIndex  Prefix: an:pl, key: {type=an,levtype=pl}
    Files:
      0 => /data/fdb/rd:xxxx:oper:20160907:0000:g/an:pl.20190607.142831.glados.2765958938627.data
    Axes:
      levelist
        1000
        300
        ...
        param
        130
        ...
        step
        0
        
    Contents of index:
      Fingerprint: 0:1000:130, location: FieldRefLocation(pathid=0,offset=0)
      Fingerprint: 0:300:130, location: FieldRefLocation(pathid=0,offset=3280398)
      ...