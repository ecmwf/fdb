fdb read
========

Read data from the FDB and write this data into a specified target file. This may involve visiting multiple databases if required by the request.

Usage
-----
::

  fdb read request.mars target.grib
  fdb read --raw request.mars target.grib
  fdb read --extract source.grib target.grib

Options
-------

+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--extract``                          | Extract the request(s) from an existing GRIB file                                       |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--statistics``                       | Report timing statistics                                                                |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename.                                                             |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--raw``                              | Uses the raw request without expansion                                                  |
+----------------------------------------+-----------------------------------------------------------------------------------------+

Examples
--------

Specify the MARS request in a plain text file.  
Note that this MARS request must be fully expanded. For example, it may not contain the **/to/** or **/by/** statements that the MARS client is able to expand.
::

  % cat myrequest
  retrieve,class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g,type=an,levtype=pl,step=0,levelist=700,param=155
  
  # this will retrieve 2 fields
  % fdb read myrequest foo.grib
  retrieve,class=od,date=20151004,domain=g,expver=0001,levelist=500/700,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an
  Compress handle: 3.3e-05 second elapsed, 3.2e-05 second cpu
  Compress handle: 2e-06 second elapsed, 2e-06 second cpu
  Read  rate: 4.7575 Gbytes per second
  Write rate: 2.54081 Gbytes per second
  Save into: 0.022224 second elapsed, 0.022182 second cpu


Obtain data from the FDB using the MARS request that would be implied by an existing GRIB file. In this example, foo.grib contains 2 fields that identify what needs to be retrieved.
::

  % fdb read --extract foo.grib out.grib
  retrieve,class=od,date=20151004,domain=g,expver=0001,levelist=500,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an
  ...
  Compress handle: 3.3e-05 second elapsed, 3.2e-05 second cpu
  Compress handle: 2e-06 second elapsed, 2e-06 second cpu
  Read  rate: 4.7575 Gbytes per second
  Write rate: 2.54081 Gbytes per second
  Save into: 0.022224 second elapsed, 0.022182 second cpu
