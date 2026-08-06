Architecture and Design
=======================

Z3FDB is built as a thin Python layer on top of a C++ core. The stack has
five layers, grouped into two categories: pure-Python code you can read and
modify without a C++ toolchain (shown in green), and compiled native code
that handles performance-critical work (shown in red/blue).

.. mermaid::
   :align: center

   %%{config: {'block': {'useMaxWidth': true}}}%%
   block-beta
   columns 1
     a["z3fdb\n[Pure Python]"]
     b["pychunked_data_view\n[Pure Python]"]
     c["chunked_data_view_bindings.cpython.so\n[Native Python bindings]"]
     d["libchunked_data_view.so\n[Chunked access of data in view]"]
     e["libfdb5.so"]
     style a fill:#cfc
     style b fill:#cfc
     style c fill:#fcc
     style d fill:#fcc
     style e fill:#ccf

Layer descriptions
------------------

**z3fdb** (pure Python)
   The user-facing API. :class:`~z3fdb.SimpleStoreBuilder` collects MARS
   requests and axis definitions, then wraps the result in a Zarr-compatible
   store object that can be opened with ``zarr.open_array()``.

**pychunked_data_view** (pure Python)
   Python types that mirror the C++ core: :class:`~pychunked_data_view.AxisDefinition`,
   :class:`~pychunked_data_view.Chunking`, :class:`~pychunked_data_view.ChunkedDataViewBuilder`,
   and :class:`~pychunked_data_view.ChunkedDataView`. This layer translates
   between Python conventions and the native bindings layer below it.

**chunked_data_view_bindings** (compiled extension)
   A ``pybind11``-generated ``.so`` that exposes the C++ types and methods
   to Python. This is the bridge between the pure-Python layers above and
   the C++ library below.

**libchunked_data_view** (compiled C++ library)
   The core of Z3FDB. It manages the ``ViewPart`` objects that represent
   individual MARS requests, computes bounding-box intersections when a
   chunk is accessed, issues sub-requests to FDB, and decodes the returned
   GRIB data into the flat float32 chunk buffer.

**libfdb5** (compiled C++ library)
   The FDB storage engine. ``libchunked_data_view`` calls into it to
   retrieve GRIB fields by MARS key.
