FDB Tools
=========

FDB has a set of general purpose CLI tools for archiving and retrieving user data, some special purpose tools for administrative maintenance and for developer tools for debugging.

The general purpose tools are:

:fdb_: A wrapper command for the tools described in this section (FDB5 Tools)
:fdb-list_: Lists the contents of the FDB databases. Data are filtered according a MARS_ request
:grib2fdb_: Inserts data into the FDB, creating a new databases if needed.
:fdb-info_: Get information about the FDB configuration and binaries
:fdb-purge_: Purge duplicate entries from the database and remove the associated data (if the data is owned, not adopted).
:fdb-schema_: Print the schema that an instance of the FDB software is configured to use
:fdb-stats_: Prints information about FDB databases
:fdb-where_: Print the location of FDB5 database
:fdb-wipe_: Deletes FDB databases and the data therein contained. Uses the passed request to identify the database to delete


The special purpose tools are:

:fdb-hide_: Hide the contents of one FDB database.
:fdb-move_: Move the content of one FDB database.
:fdb-overlay_: Make the contents of one FDB database available as though they were archived using different keys
:fdb-root_: Find the location on disk of the specified databases.


The debugging tools are:

:fdb-read_: Read data from the FDB and write this data into a specified target file.
:fdb-write_: Inserts data into the FDB, creating a new databases if needed.
:fdb-dump_: Dump the structural contents of the FDB.
:fdb-dump-index_: Dump the contents of a particular index file for debugging purposes.
:fdb-dump-toc_: Dump the structural contents of a particular toc file (or subtoc file).
:fdb-hammer_: Mimic the load produced by a writing process (I/O server) within a forecast simulation.
:fdb-patch_: Rearchive the FDB corresponding to a given request into another, whilst modifying the class or expver elements of the database key.
:fdb-reconsolidate-toc_: This is an advanced tool that exists to assist cleanup in a situation where databases have been poorly written.


.. _fdb-write: write.rst
.. _fdb-list: list.rst
.. _fdb-read: read.rst
.. _MARS: mars.rst
.. _fdb: GeneralPurposeTools/fdb.rst
.. _grib2fdb: GeneralPurposeTools/grib2fdb.rst
.. _fdb-info: GeneralPurposeTools/info.rst
.. _fdb-purge: GeneralPurposeTools/purge.rst
.. _fdb-schema: GeneralPurposeTools/schema.rst
.. _fdb-stats: GeneralPurposeTools/fdb-stats.rst
.. _fdb-where: GeneralPurposeTools/where.rst
.. _fdb-wipe: GeneralPurposeTools/wipe.rst
.. _fdb-dump: DebugTools/dump.rst
.. _fdb-dump-index: DebugTools/dump-index.rst
.. _fdb-dump-toc: DebugTools/dump-toc.rst
.. _fdb-hammer: DebugTools/hammer.rst
.. _fdb-patch: DebugTools/patch.rst
.. _fdb-reconsolidate-toc: DebugTools/reconsolidate-toc.rst
.. _fdb-hide: SpecialPurposeTools/hide.rst
.. _fdb-move: SpecialPurposeTools/move.rst
.. _fdb-overlay: SpecialPurposeTools/overlay.rst
.. _fdb-root: SpecialPurposeTools/root.rst
