fdb write
=========

Inserts data into the FDB, creating a new databases if needed.  
The data is copied into the FDB, and the tool reports the location where it was inserted.  
This process is atomic and can be run concurrently to other processes reading or writing to the same FDB databases.

Usage
-----

``fdb write [--filter-include=...] [--filter-exclude=...] <path1> [path2] ...``

Options
-------

+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--verbose``	                         | Prints more information, namely the key of each datum and the                           |
|                                        | information of which data was filtered out                                              |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--statistics``                       | Report timing statistics                                                                |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--include-filter=string,...``        | | Filter out any data that **does not match** this key-value pairs.                     |
|                                        | | Key-value pairs can be in the form of a MARS request, e.g.: ``step=141/to/240/by/3``  |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--exclude-filter=string,...``        | | Filter out any data that **does match** this key-value pair.                          | 
|                                        | | Key-value pairs can be in the form of a MARS request, e.g.: ``levelist=850/1000``     |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename.                                                             |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--modifiers=string``                 | List of comma separated key-values of modifiers to each message int input data          |
+----------------------------------------+-----------------------------------------------------------------------------------------+

Examples
--------

You may pass multiple grib files. The tool will insert them sequentially.
::

  % fdb write data.grib
  
  Processing data.grib
  FDB archive 12 fields, size 37.5412 Mbytes, in 0.088939 second (422.091 Mbytes per second)
  fdb::service::archive: 0.089006 second elapsed, 0.089005 second cpu



Use ``--include-filter`` you have a large data set of which you want to write a small sub-set easily identifiable from a few keys ...
::

  % fdb write --verbose --include-filter=time=0000 data.grib
 
  Processing data.grib
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
  Include key {time=0000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
  Include key {time=0000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an}
  Include key {time=0000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
  Include key {time=0000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an}
  Archiving {class=rd,date=20160401,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
  ...
  FDB archive 8 fields, size 25.0275 Mbytes, in 0.129475 second (193.301 Mbytes per seconds)
  fdb::service::archive: 0.129522 second elapsed, 0.129514 second cpu



Use ``--exclude-filter`` you have a large data set of which you want to filter out a small sub-set easily identifiable from a few keys ...
::

  % fdb write --verbose --exclude-filter=time=1200,levelist=1000 data.grib
  
  Processing data.grib
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
  Exclude key {time=1200,levelist=1000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
  Exclude key {time=1200,levelist=1000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an}
  Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
  Archiving {class=rd,date=20160401,domain=g,expver=xxxx,levelist=850,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
  Exclude key {time=1200,levelist=1000} filtered out datum {class=rd,date=20160401,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
  Exclude key {time=1200,levelist=1000} filtered out datum {class=rd,date=20160401,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an}
  ...
  FDB archive 12 fields, size 37.5412 Mbytes, in 0.160719 second (233.584 Mbytes per seconds)
  fdb::service::archive: 0.160764 second elapsed, 0.160724 second cpu
