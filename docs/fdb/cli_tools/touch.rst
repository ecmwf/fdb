fdb touch
=========

Explicitly create or update the last access (read or write) records for a database or databases.

If a database was created with an older version of the FDB, and does not have a last access
record, this tool will attempt to create it. This will require write permissions to relevant
database equivalent to the original creator/owner of the database.

Usage
-----

``fdb touch --read request1 [request2] ...``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--read``                             | Update or create the read timestamp for each matching database.                                                     |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--write``                            | Update or create the write timestamp for each matching database.                                                    |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--time=<when>``                      | | Record ``<when>`` rather than the current time.                                                                   |
|                                        | | ``YYYYMMDD``, ``YYYY-MM-DD``, ``YYYYMMDDHHMMSS`` or ``YYYY-MM-DDTHH:MM:SS``, interpreted as UTC.                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--from-toc``                         | | Record the modification time of the database's toc rather than the current time.                                  |
|                                        | | Use this to seed an archive that predates access timestamps, so that it does not all appear freshly accessed.     |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | Visit all FDB databases.                                                                                            |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--minimum-keys=string,string``       | The minimum keys that must be specified to run the tool. Default is ``class,expver,stream,date,time``               |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--raw``                              | Don't apply (contextual) expansion and checking on requests.                                                        |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename.                                                                                         |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+

An invocation that specifies neither ``--read`` nor ``--write`` is an error, as is specifying both
``--time`` and ``--from-toc`` — they both choose which instant to record.

If the database is configured with a backend which does not support access timestamp records, this
tool is a no-op.

Permissions
-----------

Although updating the timestamps to the current time can be done by any user, either creating
imestamps where they do not exist, or explicitly setting them to a different value, requires
 ownership of the database.

Example 1
---------

Update both read and write access timestamps to the current time
::

  % fdb touch --read --write class=rd,expver=wxyz,stream=oper,date=-1,time=0000,domain=g

  Created access timestamps for {class=rd,expver=wxyz,stream=oper,date=20261009,time=0000,domain=g}
  Created access timestamps for 1 database(s)

Example 2
---------

Create the read and write access timestamps for an existing dataset which does not have them, using
the last modification time of the toc as the template time
::

  % fdb touch --read --write --from-toc class=rd,expver=wxyz,stream=oper,date=-1,time=0000,domain=g

  Created access timestamps for {class=rd,expver=wxyz,stream=oper,date=20261009,time=0000,domain=g}
  Created access timestamps for 1 database(s)
