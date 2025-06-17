fdb overlay
===========

.. warning::
    This is a **powerful and advanced** command. With power comes **responsibility**.

    This command has the potential to hide data, and **cannot be used repeatedly**.

    It should only be used if you **fully** understand its implications.

Make the contents of one FDB database available as though they were archived using different keys (class or expver).

The new database behaves as though it has a symlink to the old, and any data retrieved will have the relevant keys updated on retrieval (on the fly).

Because data is rewritten on the fly, there may be a significant performance penalty when reading data from an overlaid FDB.

Usage
-----

``fdb overlay [options] [source DB request] [target DB request]``

Options
-------

At least one of class or expver is required is required

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--force``                            | Apply overlay even if the target database already exists                                                            |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--remove``                           | Remove a previously applied FDB overlay. Once an overlay has been removed, it cannot be reapplied.                  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--variable-keys=string,string``      | | Specify the keys that may vary between mounted DBs.                                                               |
|                                        | | Defaults: class,expver                                                                                            |
|                                        | | Currently the code that sets values on the GRIB messages only supports string values. If the set of               |
|                                        | | keys is extended, the user must ensure that only string type keys are used.                                       |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename                                                                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+

Example
-------

Add overlay
::

  % fdb overlay class=rd,expver=xxxx,stream=oper,date=20160907,time=0000 class=rd,expver=xxxz,stream=oper,date=20160907,time=0000
  Applying {class=rd,expver=xxxx,stream=oper,date=20160907,time=0000,domain=g} (Rule[line=127]) onto {class=rd,expver=xxxz,stream=oper,date=20160907,time=0000,domain=g} (Rule[line=127])

This overlay can be removed
::
  
  % fdb overlay --remove class=rd,expver=xxxx,stream=oper,date=20160907,time=0000 class=rd,expver=xxxz,stream=oper,date=20160907,time=0000
  Removing {class=rd,expver=xxxx,stream=oper,date=20160907,time=0000,domain=g} (Rule[line=127]) from {class=rd,expver=xxxz,stream=oper,date=20160907,time=0000,domain=g} (Rule[line=127])



