##############
Content of src
##############

****
Fdb5
****

`fdb5/` -> Fields Database v5 source code.

**********
Daos Dummy
**********

`daos_dummy/` -> DAOs mock.

*****************
Chunked Data View
*****************

`chunked_data_view/` -> Chunked Data View into a FDB5.

**************************
Chunked Data View Bindings
**************************

`chunked_data_view_bindings/` -> Python bindings for 'Chunked Data View',
internal use only.

************************
Python Chunked Data View
************************

`pychunked_data_view/` -> Python wrapper around python bindings, public
interfacte to a 'Chunked Data View'. Having a pure-python wrapper enables IDEs
to aceess pydoc without having to change security settings to load our
pychunked\_data\_view.so.

*****
z3fdb
*****

Zarr v3 api data store implementation
