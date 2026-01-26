.. _Z3FDB_Introduction:

########################
Z3FDB
########################

:Version: |version|

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :hidden:

   architecture
   api

Introduction
############

Z3FDB enables FDB to function as a Zarr Store without the need to copy your
GRIB data. Z3FDB uses the MARS Language to define views that then can be
accessed as a store.

When a Zarr chunk is accessed, a corresponding retrieve call will be done on
FDB and the returned GRIB data will be decoded on the fly. Data will be
represented as float32.

.. admonition:: Suported data

   FDB can store arbitray data, not just GRIB.
   At this point Z3FDB only supports data extraction from GRIB.
   It is the responsibility of the caller to ensure that only GRIB data is
   accessed through Z3FDB.

Use Case for Z3FDB
##################

You should consider using Z3FDB when you can afford a 2x-5x slowdown compared
to reading on-disk Zarr stores. This can be the right trade-off in the
following scenarios:


* **Prototyping with existing FDB**

  If you already have GRIB data inside an FDB instance and want to prototype
  against a dynamically defined Zarr stores. Changing the content of your store
  simply requires creating a new store with a differnt configuration. This can
  be very convenient if you otherwise would need to extract data from FDB and
  then write out a Zarr store to disk. Creating Z3FDB stores is cheap because
  data is only accessed when the corresponding Zarr chunk is accessed.

* **Very Large Data Stores**

  You may have much more data available in FDB than you can redundantly store
  as GRIB and Zarr. In this case Z3FDB allows you access GRIB data in Zarr
  representation without additional storage requirements.


Installation
############

Z3FDB requires libfdb5 to be present on your system during runtime.

During the `cmake` step, set the following variable:
`-DENABLE_PYTHON_ZARR_INTERFACE=ON`.

After building the bundle with `ninja` or `make`, a python project is created
in your `CMAKE_BINARY_DIR` called '`staging...`'. Change into this directory
and install the `Z3FDB` as usual.

.. code-block:: bash

   pip install .


To check if the install was successful, run `pytest` in
`<FDB_SRC_FOLDER>/tests/z3fdb`.

Examples
########

Below are two examples how to create a view

.. code-block:: python
   :caption: Configuring a view from a single MARS request.

   from z3fdb import (
       SimpleStoreBuilder, 
       AxisDefinition,
       Chunking,
       ExtractorType
       SimpleStoreBuilder,
   )
   builder = SimpleStoreBuilder()
   builder.add_part(
       "class=od,"
       "domain=g,"
       "date=-2/-1,"
       "expver=0001,"
       "levtype=sfc,"
       "param=167,"
       "step=0/1/2/3/4/5/6,"
       "stream=oper,"
       "time=0000/1200,"
       "type=fc",
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
           AxisDefinition(["step"], Chunking.SINGLE_VALUE)
       ],
       ExtractorType.GRIB,
   )
   store = builder.build()
   data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

   # Access full field for date=-1, time=0000, step=3
   field = data[2][3][:]

.. code-block:: python
   :caption: Configuring a view from multiple MARS request.

   from z3fdb import (
       AxisDefinition,
       Chunking,
       ExtractorType
       SimpleStoreBuilder,
   )
   builder = SimpleStoreBuilder()
   builder.add_part(
       "type=an,"
       "class=ea,"
       "domain=g,"
       "expver=0001,"
       "stream=oper,"
       "date=2020-01-01/2020-01-02,"
       "levtype=sfc,"
       "step=0,"
       "param=165/166,"
       "time=0/to/21/by/3",
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
           AxisDefinition(["param"], Chunking.SINGLE_VALUE)
       ],
       ExtractorType.GRIB,
   )
   builder.add_part(
       "type=an,"
       "class=ea,"
       "domain=g,"
       "expver=0001,"
       "stream=oper,"
       "date=2020-01-01/2020-01-02,"
       "levtype=pl,"
       "step=0,"
       "param=131/132,"
       "levelist=50/100,"
       "time=0/to/21/by/3",
       [
           AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
           AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE)
       ],
       ExtractorType.GRIB,
   )
   builder.extendOnAxis(1)
   store = builder.build()
   data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

   # Access full field for date=2020-01-01, time=0300, param=166, uses first part
   field = data[1][1][:]

   # Access full field for date=2020-01-01, time=0300, param=131, levelist=100, uses second part
   field = data[1][3][:]
