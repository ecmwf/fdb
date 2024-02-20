Checking Installation
=================================

To check whether the installation of the **FDB** was successful we can execute
the following command

.. code-block:: console

   which fdb

This should show the path to the installed FDB instance. If you receive and error
message at this point in time, make sure that you added the path FDB is located
at to you `PATH` environment variable.

.. code-block:: console

   fdb help

The help argument shows a list of options we have to interact with the FDB application.
The most useful for this stage would be to run ``fdb home`` to check whether the configuration
of FDB and the corresponding home directory was successful. Running this command
results in the following output:

.. code-block:: console

   /path/to/fdb/home

If this isn't the case, make sure you exported the ``FDB_HOME`` environment variable
as stated in the installation guide. Next we want to show information about
the FDB instance. We run

.. code-block:: console

   fdb schema

This should print the set up schema.

