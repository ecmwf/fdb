fdb wipe
========

Deletes FDB databases and the data therein contained. Uses the passed request to identify the database to delete.

This is equivalent to a UNIX rm command.

It expects a minimum set of keys to be set, but it can be overridden with the option --minimum-keys.

This tool deletes either whole databases, or whole indexes within databases

Usage
-----

``fdb wipe request1 [request2] ...``

``fdb wipe --doit request1 [request2] ...``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--doit``                             | Actually delete the files.                                                                                          |
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

Example 1
---------

List data for deletion according to a specified key
::

  % fdb wipe class=rd,expver=wxyz,stream=oper,date=20151004,time=1200

  Wiping for request
  retrieve,
    class=rd,
    expver=wxyz,
    stream=oper,
    date=20151004,
    time=1200

  FDB owner: <id>

  Metadata files to delete:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/toc    /data/fdb5/rd:wxyz:oper:20151004:1200:g/schema
  Data files to delete:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665731.data

  Index files to be deleted:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665730.index

  Other files:
    - NONE -

  ...

  Rerun command with --doit flag to delete unused files
  
Example 2
---------

And actually delete the data listed in example 1
::

  % fdb wipe --doit class=rd,expver=wxyz,stream=oper,date=20151004,time=1200

  Wiping for request
  retrieve,
    class=rd,
    expver=wxyz,
    stream=oper,
    date=20151004,
    time=1200

  FDB owner: <id>

  Metadata files to delete:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/toc    /data/fdb5/rd:wxyz:oper:20151004:1200:g/schema
  Data files to delete:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665731.data

  Index files to be deleted:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665730.index

  Other files:
    - NONE -

  ...


  Unlink /data/fdb5/rd:wxyz:oper:20151004:1200:g/toc
  Unlink /data/fdb5/rd:wxyz:oper:20151004:1200:g/schema
  Unlink /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665731.data
  Unlink /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665730.index
  ...

Example 3
---------

The same example as before, but specifying a smaller sub-set of keys:

Use --minimum-keys with caution! Setting --minimum-keys=class is a BAD IDEA! You risk deleting the whole FDB.
::
  
  % fdb wipe --doit --minimum-keys=class,expver class=rd,expver=wxyz

  Wiping for request
  retrieve,
    class=rd,
    expver=wxyz

  FDB owner: <id>

  Metadata files to delete:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/toc    /data/fdb5/rd:wxyz:oper:20151004:1200:g/schema
  Data files to delete:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665731.data

  Index files to be deleted:
    /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665730.index

  Other files:
    - NONE -

  ...

  Unlink /data/fdb5/rd:wxyz:oper:20151004:1200:g/toc
  Unlink /data/fdb5/rd:wxyz:oper:20151004:1200:g/schema
  Unlink /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665731.data
  Unlink /data/fdb5/rd:wxyz:oper:20151004:1200:g/an:pl.20170323.164359.host.30249454665730.index