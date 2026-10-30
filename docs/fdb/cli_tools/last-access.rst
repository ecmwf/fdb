fdb last-access
===============

Reports the last access and modification timestamps for each matching FDB database. Meaningful access in this
context means *read* or *write* (pure metadata operations are not included).

A database with no timestamp, for example one produced with an older version of the FDB, is marked as
``UNKNOWN``, and is omitted by the ``--older-than`` and ``--newer-than`` tests to avoid data being incorrectly
removed as stale. It is still reported when no such filter is given.

Usage
-----

``fdb last-access request1 [request2] ...``

``fdb last-access --all --older-than=90d``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--read``                             | Consider only the read timestamp. May not be combined with ``--write``.                                             |
|                                        | In the absence of ``--read`` or ``--write`` the most recent timestamp is used                                       |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--write``                            | Consider only the write timestamp. May not be combined with ``--read``.                                             |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--older-than=duration``              | Only report databases not accessed within this period.                                                              |
|                                        | Durations of the form "5d12h" with smhd supported. A bare number is interpreted as hours.                           |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--newer-than=duration``              | Only report databases accessed within this period.                                                                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--unknown``                          | Only report databases that have no timestamps, and whose access history is therefore unknown.                       |
|                                        | Cannot be combined with ``--older-than`` or ``--newer-than``.                                                       |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--simple``                           | Simple output. A tabular format containing only the age and the matching request                                    |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--json``                             | JSON serialised output for easy downstream parsing. A list of objects carrying the age in seconds,                  |
|                                        | both timestamps as epoch seconds, the location and the key. May not be combined with ``--simple``                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--wipe``                             | Delete the databases the query selected. Requires ``--older-than``. Without ``--doit`` this performs a dry run.     |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--doit``                             | Actually perform a wipe.                                                                                            |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--ignore-errors``                    | Report errors as warnings and continue. With ``--wipe``, a database that cannot be                                  |
|                                        | deleted cleanly is then skipped rather than stopping the sweep.                                                     |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | Visit all FDB databases.                                                                                            |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--raw``                              | Don't apply (contextual) expansion and checking on requests.                                                        |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename.                                                                                         |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+

It is not an error for a filter to match nothing. An error is raised only if the *request* matches no database at all.

Example 1
---------

Find databases neither read nor written in the last 90 days
::

  % fdb last-access --older-than=90d

    database:   {class=od,expver=0001,stream=oper,date=20240911,time=0000,domain=g}
    location:   /data/fdb5/od:0001:oper:20240911:0000:g
    last read:  2024-12-31 22:00:00
    last write: 2024-09-11 06:00:00
    age:        281 days

Example 2
---------

Find databases that are still being written but nobody reads — candidates for a retention review
::

  % fdb last-access --read --older-than=364d --simple --all

  40348800 {class=od,expver=0001,stream=oper,date=20240911,time=0000,domain=g}

Example 3
---------

Databases whose access history is not recorded. These must not be treated as cold
::

  % fdb last-access --unknown --json class=rd | jq

  [
    {
      "age": "UNKNOWN",
      "lastRead": 0,
      "lastWrite": 0,
      "location": "/data/fdb5/rd:xxxx:oper:20200101:0000:g",
      "key": {
        "class": "rd",
        "expver": "xxxx",
        "stream": "oper",
        "date": "20200101",
        "time": "0000",
        "domain": "g"
      }
    }
  ]

Example 4
---------

Delete matching data which has not been touched for a year
::

  % fdb last-access --older-than=364d --wipe class=rd,expver=xxxx

    database:   {class=rd,expver=xxxx,stream=oper,date=20201102,time=0000,domain=g}
    location:   /data/fdb5/rd:xxxx:oper:20201102:0000:g
    last read:  2020-01-01 00:00:00
    last write: 2020-01-01 00:00:00
    age:        2480 days
  [Database: rd:xxxx:oper:20201102:0000:g, FDB Owner: fdbuser]
  Catalogue URIs to delete:
      /data/fdb5/rd:xxxx:oper:20201102:0000:g/schema
      /data/fdb5/rd:xxxx:oper:20201102:0000:g/toc
  ...

  1 database(s) would be wiped
  Rerun command with --doit flag to delete the data

  % fdb last-access --older-than=364d --wipe --doit class=rd,expver=xxxx
  ...
