fdb root
========

Find the location on disk of the specified databases.

Can also be used to create the database without writing to it.

Usage
-----

``fdb root [--options] [request1] [request2] ...``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--create``                           | If a DB does not exist for the provided key, create it.                                                             |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename                                                                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+


Example 1
---------

Pass a partial request (as a key) to list the location of the database (if found).
::

  % fdb root class=od,expver=0001,stream=oper,date=20160907,time=1200

  {class=od,expver=0001,stream=oper,date=20160907,time=1200,domain=g} (Rule[line=128])
  TocDBReader(/data/fdb/od:0001:oper:20160907:1200:g)

Example 2
---------

Pass a partial request (as a key) to create the location of the database. 

Note that the supplied keys must be sufficient to identify the database. This supposes some knowledge of the way the internal schema is organised, and therefore should not be used for operational purposes.
::
  
  % fdb root --create class=od,expver=0001,stream=oper,date=20160907,time=1200

  {class=od,expver=0001,stream=oper,date=20160907,time=1200,domain=g} (Rule[line=128])
  TocDBWriter(/data/fdb/od:0001:oper:20160907:1200:g)