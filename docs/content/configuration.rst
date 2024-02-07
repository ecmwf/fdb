Configuration
-------------

First thing you should do after the installation is the setting up of the home
directory for FDB. Choose a directory. We will refer to it as ${FDB_HOME}.

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

.. _Config: config-schema.html
.. _Schema: config-schema.html#schema
