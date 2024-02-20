Configuration
=============

First thing you should do after the installation is the setting up of the home
Export the environment variable as follows:

::

   export FDB_HOME="/path/to/wished/fdb/home"

If you add it to your ``.bashrc`` or ``.zshrc``, source the file or restart the 
terminal session.
Also export the environment variable ``${FDB_HOME}`` and create the following 
structure within the FDB home directory:

.. code-block:: text
   
   FDB_HOME
      └──etc
         └──fdb
            ├──config.yaml
            └──schema

The ``config.yaml`` and ``schema`` files are the files which can be found in the
corresponding sections of this document for config_ or schema_.

FDB is looking for the ``schema`` file in the global FDB home directory. The 
specific path it looks for needs to be set as shown above. This global scheme is
used for storing data into the FDB database. In case data has already been store
in the FDB a local copy of the schema used during the storing process was created
and is used for further storage operations. This is to guarantee data consistency
over the course of several storage procedures even if the global schema is subject
to changes.

Checking Installation
---------------------------------

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




.. _Config: config-schema.html
.. _Schema: config-schema.html#schema
