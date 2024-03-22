Architecture
============

Client API & Semantics
----------------------
The client side API of the FDB exposes several functions for data handling. The
following section lists each of the high-level functions and shortly describes
its meaning.

archive()
`````````
Archives a given message set with respect to a given key and creates a new database if
necessary. 

The key, describing the data in a globally unique and minimal manner,
is created from the metadata describing the data, e.g. the meteorological
information attached to a GRIB message. The schema is consolidated to determine how
the data layout in the FDB's store should be created. After the persistence of the data a new
entry in the catalogue is created.

flush()
```````
Ensures data is persisted on disk. This is especially important in cases where the
data is stored remotely. 

Blocks until all data is persisted into the FDB's store.

list()
``````
List the contents of the FDB databases.

Duplicate data, e.g. such data which has been masked by newer entries given the same key,
is skipped.

purge()
```````
Purge duplicate entries from the catalogue and remove the associated data
which is matching with a given request.

Data in the FDB is immutable. It is masked, but not damaged or deleted, when
it is overwritten with new data given the same key. Due to this property, duplicates
of the data can be present at any time. Purging leads to the removal of redundant
data without compromising already saved entries.

This only holds for data which was written by the user who trys to delete it.


Catalogue
---------


Store
-----

Data Routing
------------

Remote
------

Authentication & Security
-------------------------
<Placeholder>
