fdb hide
========

Hide the contents of one FDB database. This masks all existing entries in the database such that they are permanently inaccessible, without destructively damaging the data or indexes.

This tool is envisaged for two contexts:

Operational implementation day, to assist rebadging of hindcast data
Hiding the contents of a damaged FDB in time-critical circumstances to allow rerunning whilst also preserving a trail for later diagnostics.

Usage
-----

``fdb hide [options] [DB request]``

Options
-------
Database request must match exactly one database.

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--doit``                             | Do the actual change                                                                                                |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename                                                                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+


Example
-------

This tool is non-destructive by default (only indicates the changes that will be made)
::

  % fdb hide class=rd,expver=xxxx,stream=oper,date=20160907,time=0000,domain=g
  Hide contents of DB: TocDBReader(<path>/fdb5/root/rd:xxxx:oper:20160907:0000:g)
  Run with --doit to make changes

These changes can then be made permanent
::
  
  % fdb hide --doit class=rd,expver=xxxx,stream=oper,date=20160907,time=0000,domain=g
  Hide contents of DB: TocDBReader(<path>/fdb5/root/rd:xxxx:oper:20160907:0000:g)