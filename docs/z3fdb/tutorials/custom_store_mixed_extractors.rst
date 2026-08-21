.. SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
.. SPDX-License-Identifier: Apache-2.0

.. _tutorial_custom_store_mixed_extractors:

Mixed-Extractor Custom Store
=============================

This tutorial shows how to use :class:`~z3fdb.CustomStoreBuilder` to create
a single Zarr store that contains multiple named arrays, each backed by a
different extraction strategy. You will build a store with two arrays holding
the same 10 m u-wind field -- one using the standard ``Grib`` extractor for
efficient full-field access, and one using ``GribJump`` -- then plot the field
as a global map.

.. contents:: On this page
   :local:
   :depth: 1

When to use CustomStoreBuilder
-------------------------------

:class:`~z3fdb.SimpleStoreBuilder` always places a single array at the store
root. :class:`~z3fdb.CustomStoreBuilder` lifts that restriction: you can
register any number of arrays at named paths inside one store and open them
as a Zarr group.

This is useful when:

* You want to expose multiple parameters or level types through one store
  object.
* Different arrays in the store benefit from different extractors -- for
  example, surface fields used for map visualisation work well with a
  ``Grib`` extractor (one FDB retrieve returns the full field), while the
  same field accessed at individual grid points is better served by
  ``GribJump``.

The MARS Request
-----------------

The example below retrieves the 10 m u-wind (param ``165.128``) from a
single ensemble member at analysis time:

.. code-block:: python

   _REQUEST = {
       "class":   "od",
       "date":    "20260818",
       "expver":  "0001",
       "levtype": "sfc",
       "domain":  "g",
       "stream":  "enfo",
       "type":    "pf",
       "number":  [1],               # one ensemble member
       "param":   ["165.128"],       # 10 m u-wind
       "step":    [0],               # analysis step
       "time":    ["00:00:00"],
   }

With a single value on every axis the resulting array has shape
``(1, 1, 1, 1, N)`` where ``N`` is the number of grid points in the GRIB
field.

Building the Custom Store
--------------------------

.. code-block:: python

   import zarr
   import numpy as np
   import matplotlib.pyplot as plt

   zarr.config.set({"async.concurrency": 1, "threading.max_workers": 1})

   from z3fdb import AxisDefinition, Chunking, ExtractorType, CustomStoreBuilder

   _AXES = [
       AxisDefinition(keys=["date", "time"], chunking=Chunking.SINGLE_VALUE),
       AxisDefinition(keys=["number"],        chunking=Chunking.SINGLE_VALUE),
       AxisDefinition(keys=["step"],          chunking=Chunking.SINGLE_VALUE),
       AxisDefinition(keys=["param"],         chunking=Chunking.SINGLE_VALUE),
   ]

   builder = CustomStoreBuilder()

   # Array 1 - full-field Grib extraction: one FDB retrieve gives the complete field.
   # Best choice when you need all grid points (e.g. for maps).
   builder.add_part("grib/u10",    _REQUEST, axes=_AXES, extractor=ExtractorType.Grib())

   # Array 2 - GribJump extraction with one grid point per chunk.
   # Best choice for sparse access (e.g. time series at a station).
   builder.add_part("gribjump/u10", _REQUEST, axes=_AXES,
                    extractor=ExtractorType.GribJump(
                        chunking=Chunking.FixedSizeChunk(chunk_shape=1)
                    ))

   store = builder.build()

``add_part("grib/u10", ...)``
   Registers an array at the path ``grib/u10`` inside the store. The path
   follows zarr conventions: ``/``-separated segments, leading slash
   optional. Here ``grib`` becomes an intermediate group and ``u10`` is the
   array name.

``ExtractorType.Grib()``
   Standard extraction: one FDB retrieve per chunk, returning a full GRIB
   field decoded to float32. With ``SINGLE_VALUE`` chunking on every explicit
   axis, each chunk holds one complete field -- ideal for reading all grid
   points at once.

``ExtractorType.GribJump(chunking=Chunking.FixedSizeChunk(chunk_shape=1))``
   GribJump jumps to the bytes inside each GRIB message that correspond to
   the requested grid points, without decoding the rest. ``FixedSizeChunk(1)``
   makes every grid point its own chunk: reading ``arr[..., k]`` extracts
   exactly the value at grid point ``k``. This is expensive for full-field
   access but efficient for sparse access across many time steps.

Opening the Store
-----------------

Because the store contains multiple named arrays, open it as a **group**, not
an array:

.. code-block:: python

   grp = zarr.open_group(store, mode="r")

   # Access each array by its registered path
   grib_arr     = grp["grib/u10"]
   gribjump_arr = grp["gribjump/u10"]

   print("shape :", grib_arr.shape)    # (1, 1, 1, 1, N)
   print("chunks:", grib_arr.chunks)   # (1, 1, 1, 1, N) - whole field per chunk

.. code-block:: text

   grp["grib/u10"][dt, member, step, param, grid_point]
                    ^      ^     ^      ^        ^
                    |      |     |      |        grid point [implicit, size N]
                    |      |     |      param [size 1]
                    |      |     step [size 1]
                    |      member [size 1]
                    date x time [size 1]

**No data is fetched from FDB until you index.** Building and opening the
store only validates the MARS request and determines the field layout.

Plotting the 10 m u-wind Map
------------------------------

Read the full field from the ``Grib``-backed array and plot it as a global
map. Z3FDB returns a flat 1-D float32 array of ``N`` grid-point values;
reshaping it to 2-D requires the latitude and longitude coordinates for your
grid. These can be obtained from the GRIB message metadata via ``eccodes``
or ``cfgrib``.

.. code-block:: python

   # Retrieve the full field - shape (1, 1, 1, 1, N), index to (N,)
   u10_flat = grib_arr[0, 0, 0, 0, :]

   # Obtain lat/lon coordinates for your grid.
   # The example below assumes a regular 0.25 deg global grid (1440 x 721).
   # Replace nlat, nlon, lats, lons with values matching your actual grid.
   nlat, nlon = 721, 1440
   lats = np.linspace(90, -90, nlat)
   lons = np.linspace(0, 360, nlon, endpoint=False)
   u10_2d = u10_flat.reshape(nlat, nlon)

   fig, ax = plt.subplots(figsize=(12, 5))
   img = ax.pcolormesh(lons, lats, u10_2d, cmap="RdBu_r", shading="auto")
   fig.colorbar(img, ax=ax, label="10 m u-wind (m/s)")
   ax.set_xlabel("Longitude (deg)")
   ax.set_ylabel("Latitude (deg)")
   ax.set_title(f"10 m u-wind -- {_REQUEST['date']} {_REQUEST['time'][0]}, step 0, member 1")
   plt.tight_layout()
   plt.savefig("u10_map.png", dpi=150)
   plt.show()

.. note::

   For non-regular grids (e.g. the ECMWF O1280 reduced Gaussian grid) you
   cannot reshape to a simple 2-D array. In that case retrieve the
   ``(lat, lon)`` coordinates for each grid point from an ``eccodes``-opened
   sample field and pass them to :func:`matplotlib.pyplot.tricontourf`.

Next Steps
----------

* :doc:`ensemble_timeseries` -- use ``GribJump`` with ``FixedSizeChunk(1)``
  across a full ensemble and plot time series.
* :doc:`/z3fdb/dimension_mapping` -- complete reference on axis mapping,
  chunking strategies, and multi-part views.
