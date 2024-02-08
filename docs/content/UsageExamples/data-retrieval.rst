Data retrieval and FDB insertion
=====================================

This exemplary task of loading data into **FDB** should help you to familiarize 
yourself with the software.

In this scenario we want to let FDB handle a GRIB file. 

Inserting into FDB
------------------
Writing the grib file to FDB can be accomplished by the following command

::

   fdb write /path/to/grib/file

Which will result in the following output (or similiar depending on the MARS output)

::

   Processing /path/to/mars/output
   FDB archive 50 messages, size 156.422 Mbytes, in 0.350135 second (446.746 Mbytes per second)
   fdb::service::archive: 0.350308 second elapsed, 0.118118 second cpu

**WARNING: Do not run the following command
in a productive surrounding due it otherwise querying the entire FDB database.**

In case you are running an FDB instance locally (and only then) you can check the
output of ``fdb list --all --minimum-keys=""``.
