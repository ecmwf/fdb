.. SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
.. SPDX-License-Identifier: Apache-2.0

.. _tutorial_ensemble_timeseries:

Ensemble Forecast Time Series with GribJump
===========================================

This tutorial walks you through accessing ensemble forecast data from FDB
as a Zarr array. You will build a Z3FDB store backed by 50 ensemble members,
plot the 2 m temperature time series for every member, and overlay the
ensemble mean.

.. note::

   This tutorial uses ``ExtractorType.GribJump``, which is built only when fdb is configured
   with ``-DENABLE_ZARR_GRIBJUMP_EXTRACTOR=ON``, off by default. On a build without it,
   ``build()`` raises and names the flag. Check with
   ``pychunked_data_view.has_gribjump_extractor``, and see :ref:`z3fdb_gribjump_availability`.

.. contents:: On this page
   :local:
   :depth: 1

When to use GribJump
--------------------

The standard :class:`~z3fdb.ExtractorType.Grib` extractor decodes an entire
GRIB field, typically several million grid-point values, just to return the
portion you requested. For time series work where you need values at **one
or a few grid points**, this is wasteful.

:class:`~z3fdb.ExtractorType.GribJump` solves this by jumping directly to the
bytes inside each GRIB message that correspond to the requested grid points,
without decoding the rest. The trade-off: each grid-point chunk triggers an
individual GribJump lookup, so this approach shines for sparse access (a few
points across many time steps) but is slower than ``Grib`` for dense access
(full fields).

The MARS Request
----------------

The example retrieves 2 m temperature and three wind/cloud parameters from
an ECMWF ensemble forecast:

.. code-block:: python

   _STEPS = list(range(0, 91)) + list(range(93, 145, 3))
   # 0-90 h (hourly), 93-144 h (3-hourly) -> 109 steps total

   _REQUEST = {
       "class":   "od",
       "date":    "20260818",
       "expver":  "0001",
       "levtype": "sfc",
       "domain":  "g",
       "stream":  "enfo",           # ensemble forecast stream
       "type":    "pf",             # perturbed forecast members
       "number":  list(range(1, 51)),  # 50 members
       "param":   ["167.128", "165.128", "166.128", "164.128"],
                                    # 2m T, 10m u, 10m v, total cloud cover
       "step":    _STEPS,
       "time":    ["00:00:00", "06:00:00", "12:00:00", "18:00:00"],
   }

Key points:

``stream=enfo``, ``type=pf``
   Selects the ensemble stream; ``pf`` (perturbed forecast) gives you the 50
   individual members identified by ``number``.

``number``
   Each integer from 1 to 50 identifies one ensemble member.

``step``
   The forecast lead time in hours. Here we combine hourly output for the
   first 90 hours with 3-hourly output from hour 93 to 144, giving 109 steps.

``time``
   The four daily analysis times that serve as initialisation times.

Building the Store
------------------

.. code-block:: python

   import zarr
   import numpy as np
   import matplotlib.pyplot as plt

   zarr.config.set({"async.concurrency": 1, "threading.max_workers": 1})

   from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

   builder = SimpleStoreBuilder()
   builder.add_part(
       _REQUEST,
       axes=[
           AxisDefinition(keys=["date", "time"], chunking=Chunking.SINGLE_VALUE),
           AxisDefinition(keys=["number"],        chunking=Chunking.SINGLE_VALUE),
           AxisDefinition(keys=["step"],          chunking=Chunking.SINGLE_VALUE),
           AxisDefinition(keys=["param"],         chunking=Chunking.SINGLE_VALUE),
       ],
       extractor=ExtractorType.GribJump(field_chunking=Chunking.FixedSizeChunk(chunk_shape=1)),
   )
   store = builder.build()
   arr = zarr.open_array(store, mode="r")

``AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)``
   Combines date and time into a single dimension. With one date and four
   init times this gives 4 entries, ordered as time cycles within each date.

``ExtractorType.GribJump(field_chunking=Chunking.FixedSizeChunk(chunk_shape=1))``
   Uses GribJump as the extraction backend. ``FixedSizeChunk(chunk_shape=1)``
   makes each grid point its own chunk: accessing ``arr[..., k]`` retrieves
   exactly the value at grid point ``k`` without decoding the full field.

Array Shape
-----------

.. code-block:: python

   print("shape :", arr.shape)   # (4, 50, 109, 4, N)
   print("chunks:", arr.chunks)  # (1,  1,   1, 1, 1)

.. code-block:: text

   arr[dt, member, step, param, grid_point]
        ^      ^     ^      ^        ^
        |      |     |      |        grid point index [implicit, size N]
        |      |     |      param index [size 4]
        |      |     step index [size 109]
        |      member index [size 50]
        date x init time [size 4]

The **implicit final dimension** always holds the decoded grid-point values.
Its size ``N`` is determined by the GRIB grid (for a global O1280 grid,
``N ~ 6 600 000``).

**No data is fetched from FDB until you index.** Building the store is
cheap. It probes one representative field to determine the layout, but does
not retrieve the full dataset.

Plotting the Temperature Time Series
-------------------------------------

The block below retrieves the 2 m temperature time series for all 50 ensemble
members at a single grid point, computes the ensemble mean, and produces a
plot.

.. code-block:: python

   T2M        = 0          # "167.128" is first in the param list
   INIT_TIME  = 0          # 00 UTC initialisation
   GRID_POINT = 1_000_000  # replace with any valid grid-point index

   # Fetch all members at once - shape (50, 109)
   all_members   = arr[INIT_TIME, :, :, T2M, GRID_POINT]
   ensemble_mean = all_members.mean(axis=0)  # shape (109,)

   print(f"Ensemble-mean 2m temperature: {ensemble_mean.mean():.2f} K")

   fig, ax = plt.subplots(figsize=(10, 4))

   # Individual members - thin, semi-transparent
   for member_ts in all_members:
       ax.plot(_STEPS, member_ts, color="steelblue", alpha=0.2, linewidth=0.7)

   # Ensemble mean - bold
   ax.plot(
       _STEPS, ensemble_mean,
       color="darkred", linewidth=2,
       label=f"Ensemble mean ({ensemble_mean.mean():.1f} K)",
   )

   ax.set_xlabel("Forecast step (hours)")
   ax.set_ylabel("2m Temperature (K)")
   ax.set_title("2m Temperature, 50 ensemble members and mean\n"
                f"Grid point {GRID_POINT}, init {_REQUEST['date']} {_REQUEST['time'][INIT_TIME]}")
   ax.legend()
   plt.tight_layout()
   plt.savefig("t2m_timeseries.png", dpi=150)
   plt.show()

The statement ``arr[INIT_TIME, :, :, T2M, GRID_POINT]`` is a single zarr
read that triggers 50 x 109 = 5 450 GribJump extractions, one per
(member, step) combination. It returns a ``(50, 109)`` NumPy array.

.. note::

   Accessing many individual grid-point chunks in sequence can be slow for
   large ensemble x step combinations. If you need values at many grid points,
   consider increasing ``chunk_shape`` in ``FixedSizeChunk`` to batch multiple
   grid points into one GribJump call.

Next Steps
----------

* :doc:`../dimension_mapping` for the full reference on axis mapping, chunking
  strategies, fill values, and multi-part views.
* :doc:`../getting_started` for an introduction to ``SimpleStoreBuilder``
  covering surface and pressure-level data in a single array.
