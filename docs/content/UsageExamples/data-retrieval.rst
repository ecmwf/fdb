Data retrieval and FDB insertion
=====================================

This task of loading data into **FDB** should help you to familiarize 
yourself with the software.

In this scenario we want to let FDB handle a GRIB file. 

Inserting into FDB
------------------
Writing the GRIB file to FDB can be accomplished by the following command

::

   fdb write /path/to/grib/file

Which will result in the following output (or similar depending on the GRIB file)

::

   Processing /path/to/mars/output
   FDB archive 50 messages, size 156.422 Mbytes, in 0.350135 second (446.746 Mbytes per second)
   fdb::service::archive: 0.350308 second elapsed, 0.118118 second cpu

.. **WARNING: Do not run the following command
.. in a production surrounding due it otherwise querying the entire FDB database.**
..
.. In case you are running an FDB instance locally (and only then) you can check the
.. output of ``fdb list --all --minimum-keys=""``.
