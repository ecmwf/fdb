Mars data retrieval and FDB insertion
=====================================

This exemplary task of loading data into **FDB** should help you to familiarize 
yourself with the software.

Retrieving data from MARS
-------------------------

First part of the task is to retrieve some exemplary data from MARS, see mars_request_.
Connect to the HPC and run the following command

::

   mars temp_request

where ``temp_request`` could be a specified mars-request as linked above. 
The retrieved file can be found in the current working directory. We transfer it
to the machine running the fdb instance by using ``scp``.

.. _mars_request: ../mars.html

Inserting into FDB
------------------
Writing the MARS output to FDB can be accomplished by the following command

::

   fdb-write /path/to/mars/output

Which will result in the following output (or similiar depending on the MARS output)

::

   Processing /path/to/mars/output
   FDB archive 50 messages, size 156.422 Mbytes, in 0.350135 second (446.746 Mbytes per second)
   fdb::service::archive: 0.350308 second elapsed, 0.118118 second cpu

**WARNING: Do not run the following command
in a productive surrounding due it otherwise querying the entire FDB database.**

In case you are running an FDB instance locally (and only then) you can check the
output of ``fdb-list --all --minimum-keys=""``.
