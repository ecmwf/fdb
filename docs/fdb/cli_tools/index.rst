CLI Tools
=========

FDB provides CLI tools for archiving and retrieving data, administrative maintenance, and debugging.

All tools can be invoked through the :doc:`fdb <fdb>` wrapper command or directly as ``fdb-<command>``.

.. toctree::
   :hidden:

   fdb
   compare
   fdb-stats
   grib2fdb
   info
   list
   purge
   read
   schema
   where
   wipe
   write
   hide
   move
   overlay
   root
   dump
   dump-index
   dump-toc
   hammer
   patch
   reconsolidate-toc

General Purpose Tools
---------------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Command
     - Description
   * - :doc:`fdb compare <compare>`
     - Compare data between two FDBs or different data versions within one FDB.
   * - :doc:`fdb info <info>`
     - Show information about the FDB configuration and binaries.
   * - :doc:`fdb list <list>`
     - List the contents of FDB databases.
   * - :doc:`fdb purge <purge>`
     - Purge duplicate entries and remove associated data.
   * - :doc:`fdb read <read>`
     - Read data from the FDB and write it to a file.
   * - :doc:`fdb schema <schema>`
     - Print the schema used by an FDB instance or database.
   * - :doc:`fdb stats <fdb-stats>`
     - Print aggregated information about FDB databases.
   * - :doc:`fdb where <where>`
     - Print the location of an FDB database.
   * - :doc:`fdb wipe <wipe>`
     - Delete FDB databases and the data they contain.
   * - :doc:`fdb write <write>`
     - Insert data into the FDB, creating databases as needed.
   * - :doc:`grib2fdb5 <grib2fdb>`
     - Insert GRIB data into the FDB (legacy tool).

Special Purpose Tools
---------------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Command
     - Description
   * - :doc:`fdb hide <hide>`
     - Mask all entries in a database so they are permanently inaccessible, without destroying data.
   * - :doc:`fdb move <move>`
     - Lock and duplicate a database to another root.
   * - :doc:`fdb overlay <overlay>`
     - Overlay entries from one database onto another. Advanced use only.
   * - :doc:`fdb root <root>`
     - Find the on-disk location of specified databases.

Debug Tools
-----------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Command
     - Description
   * - :doc:`fdb dump <dump>`
     - Dump the structural contents of the FDB (e.g. Table of Contents entries).
   * - :doc:`fdb dump-index <dump-index>`
     - Dump the contents of a particular index file.
   * - :doc:`fdb dump-toc <dump-toc>`
     - Dump the structural contents of a TOC or subtoc file.
   * - :doc:`fdb hammer <hammer>`
     - Simulate the I/O load of a writing process within a forecast.
   * - :doc:`fdb patch <patch>`
     - Re-archive data whilst modifying the class or expver of the database key.
   * - :doc:`fdb reconsolidate-toc <reconsolidate-toc>`
     - Repair databases with poorly written or fragmented TOC entries.
