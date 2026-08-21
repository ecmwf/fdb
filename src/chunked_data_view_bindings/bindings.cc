// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/ChunkedDataView.h"
#include "chunked_data_view/ChunkedDataViewBuilder.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/LibChunkedDataView.h"
#include "chunked_data_view/extractors/grib/GribExtractorDefinition.h"
#include "chunked_data_view/extractors/gribjump/GribJumpExtractorDefinition.h"

namespace py = pybind11;
namespace cdv = chunked_data_view;

namespace docs {

inline constexpr auto chunked_data_view_module_doc = R"doc(
Low-level pybind11 bindings for the chunked_data_view C++ library.
This module exposes the core types needed to build a Zarr-compatible N-dimensional
view over FDB data. It is not part of the public API; users should import from
``pychunked_data_view`` or ``z3fdb`` instead.
Typical call sequence::
    init_bindings()
    builder = ChunkedDataViewBuilder(fdb_config_path)
    builder.add_part(mars_request_string, [AxisDefinition(...)], ExtractorType.Grib())
    view = builder.build()
    chunk = view.at([0, 1, 0])  # numpy array of float32
)doc";
};

/// Empty tag type used as the pybind11 handle for the ExtractorType namespace object.
/// cdv::ExtractorType itself has no data members
struct ExtractorTypeNamespace {};

PYBIND11_MODULE(chunked_data_view_bindings, m) {
    m.doc() = docs::chunked_data_view_module_doc;

    m.def(
        "init_bindings", []() { cdv::init_eckit_main(); },
        "Initialise the eckit main singleton.\n\n"
        "Must be called exactly once before any other function or class in this module is\n"
        "used. Subsequent calls are safe but have no effect.");

    // Build capability. ExtractorType.GribJump is always present so that user code does not
    // depend on build flags; this reports whether it can actually be used.
#ifdef HAVE_ZARR_GRIBJUMP_EXTRACTOR
    m.attr("has_gribjump_extractor") = true;
#else
    m.attr("has_gribjump_extractor") = false;
#endif

    // Exception registration. Names are re-exported from chunked_data_view_bindings/__init__.py
    // and pychunked_data_view, so user code can catch them by type.
    py::register_local_exception<chunked_data_view::GribJumpExtractorException>(m, "GribJumpExtractorError");
    py::register_local_exception<chunked_data_view::GribExtractorException>(m, "GribExtractorError");

    // Axis Definition and subclasses
    auto axis_definition = py::class_<cdv::AxisDefinition>(
        m, "AxisDefinition",
        "Maps one or more MARS keywords to a single array axis and defines how that axis\n"
        "is subdivided into Zarr chunks.\n\n"
        "One or more MARS keywords are combined into a single axis whose size is the\n"
        "Cartesian product of their individual value counts (e.g. ``['date', 'time']``\n"
        "with 2 dates and 8 times yields an axis of size 16). The chunking type then\n"
        "controls how that combined axis is split.\n\n"
        "Args:\n"
        "    keys:     Ordered list of MARS keyword names that form this axis.\n"
        "              Every listed keyword must appear in the associated MARS request\n"
        "              with at least one value.\n"
        "    chunking: One of :class:`WholeAxisChunking`, :class:`SingleValueChunking`,\n"
        "              or :class:`FixedSizeChunking`.");

    py::class_<cdv::AxisDefinition::WholeAxisChunking>(
        axis_definition, "WholeAxisChunking",
        "Chunking strategy where the entire axis is a single chunk.\n\n"
        "The Zarr chunk extent equals the full axis size. Use this when the access\n"
        "pattern always reads the whole axis (e.g. all time steps together).")
        .def(py::init<>([]() { return cdv::AxisDefinition::WholeAxisChunking{}; }));

    py::class_<cdv::AxisDefinition::SingleValueChunking>(
        axis_definition, "SingleValueChunking",
        "Chunking strategy where each element of the axis is its own chunk.\n\n"
        "The Zarr chunk extent is always 1. Use this when individual elements are\n"
        "accessed independently (e.g. one forecast step at a time).")
        .def(py::init<>([]() { return cdv::AxisDefinition::SingleValueChunking{}; }));

    py::class_<cdv::AxisDefinition::FixedSizeChunking>(
        axis_definition, "FixedSizeChunking",
        "Chunking strategy where the axis is divided into chunks of a fixed size.\n\n"
        "The chunk size must be a valid trailing-product divisor of the combined axis count\n"
        "(e.g. grouping every 3 time steps into one chunk). Use this when a tuned chunk size\n"
        "is needed to balance read amplification against the number of requests.\n\n"
        "Args:\n"
        "    chunk_size: Number of axis elements per chunk. Must be greater than zero.")
        .def(py::init<>([](size_t& chunkExtension) { return cdv::AxisDefinition::FixedSizeChunking(chunkExtension); }),
             py::arg("chunk_size"))
        .def_property_readonly(
            "chunk_shape",
            [](const cdv::AxisDefinition::FixedSizeChunking& fixedSizeChunking) { return fixedSizeChunking.chunkSize; },
            "int: Number of axis elements per chunk.");

    axis_definition
        .def(py::init([](std::vector<std::string> keys, cdv::AxisDefinition::ChunkingType chunking,
                         std::optional<std::string> name) {
                 return cdv::AxisDefinition{std::move(keys), chunking, std::move(name)};
             }),
             py::kw_only(), py::arg("keys"), py::arg("chunking"), py::arg("name") = py::none())
        .def_readwrite("keys", &cdv::AxisDefinition::keys, "list[str]: Ordered MARS keyword names that form this axis.")
        .def_readwrite("chunking", &cdv::AxisDefinition::chunking,
                       "WholeAxisChunking | SingleValueChunking | FixedSizeChunking: "
                       "Chunking strategy applied to this axis.")
        .def_readwrite("name", &cdv::AxisDefinition::name, "str | None: Optional zarr dimension name.");

    // ChunkedDataView
    py::class_<cdv::ChunkedDataView>(m, "ChunkedDataView",
                                     "Read-only N-dimensional chunked array backed by FDB field data.\n\n"
                                     "Coordinates are expressed as chunk indices: one integer per dimension\n"
                                     "identifying a chunk's position in the chunk grid. Fetching a chunk\n"
                                     "triggers one or more FDB retrievals and assembles the field values into\n"
                                     "a contiguous ``float32`` numpy array.\n\n"
                                     "The last dimension is the implicit grid-point dimension (number of values\n"
                                     "per GRIB message). It is a single chunk by default; a GribJump-backed part\n"
                                     "may subdivide it via field_chunking, in which case its chunk index varies\n"
                                     "like any other dimension.\n\n"
                                     "Instances are created exclusively by :meth:`ChunkedDataViewBuilder.build`.")
        .def(
            "at",
            [](cdv::ChunkedDataView* view, const cdv::ChunkedDataView::Index index) {
                const auto len = view->countChunkValues();
                py::array_t<float> arr(len);
                float* p = arr.mutable_data();
                {
                    py::gil_scoped_release release;
                    view->at(index, p, len);
                }
                return arr;
            },
            py::arg("index"),
            "Return the data for the chunk at *index* as a 1-D ``float32`` numpy array.\n\n"
            "Args:\n"
            "    index (list[int]): Chunk-grid coordinates, one entry per dimension,\n"
            "                       including the implicit grid-point dimension (whose index\n"
            "                       is 0 unless the part uses field_chunking).\n\n"
            "Returns:\n"
            "    numpy.ndarray: 1-D float32 array of length ``chunk_shape()[-1]``\n"
            "                   containing the field values for this chunk.\n\n"
            "Raises:\n"
            "    RuntimeError: If the FDB retrieval fails or the index is out of bounds.")
        .def(
            "chunk_shape", [](const cdv::ChunkedDataView* view) { return view->chunkShape(); },
            py::call_guard<py::gil_scoped_release>(),
            "tuple[int, ...]: Number of elements per chunk in each dimension (Zarr chunk shape).")
        .def(
            "chunks", [](const cdv::ChunkedDataView* view) { return view->chunks(); },
            py::call_guard<py::gil_scoped_release>(),
            "tuple[int, ...]: Number of chunks along each dimension of the chunk grid.")
        .def(
            "shape", [](const cdv::ChunkedDataView* view) { return view->shape(); },
            py::call_guard<py::gil_scoped_release>(),
            "tuple[int, ...]: Total number of elements along each dimension of the full array.")
        .def(
            "fill_missing_value", [](const cdv::ChunkedDataView* view) { return view->fillMissingValue(); },
            py::call_guard<py::gil_scoped_release>(),
            "float: Value written for array positions not covered by any data part\n"
            "       (default: ``float('nan')``).");

    // Extractor definitions. These *are* the user-facing configuration objects: the abstract
    // base is registered so that pybind11 knows the hierarchy, and each concrete definition is
    // exposed under the ExtractorType namespace object.
    py::class_<cdv::ExtractorDefinition>(
        m, "ExtractorDefinition",
        "Base class of every extractor configuration.\n\n"
        "Not instantiable — use :class:`ExtractorType.Grib` or :class:`ExtractorType.GribJump`.");

    auto extractor_type_ns = py::class_<ExtractorTypeNamespace>(
        m, "ExtractorType",
        "Namespace for extractor configuration types.\n\n"
        "* :class:`ExtractorType.Grib`     — standard full-field GRIB extraction.\n"
        "* :class:`ExtractorType.GribJump` — partial-field extraction via GribJump.");

    py::class_<cdv::GribExtractorDefinition, cdv::ExtractorDefinition>(
        extractor_type_ns, "Grib",
        "Configuration for full-field GRIB extraction.\n\n"
        "Args:\n"
        "    fdb_config (pathlib.Path | None): Path to the FDB configuration YAML.\n"
        "        ``None`` (default) uses the builder's FDB config.")
        .def(py::init([](std::optional<std::filesystem::path> fdbConfig) {
                 cdv::GribExtractorDefinition definition{};
                 definition.fdbConfig = std::move(fdbConfig);
                 return definition;
             }),
             py::kw_only(), py::arg("fdb_config") = py::none())
        .def_readwrite("fdb_config", &cdv::GribExtractorDefinition::fdbConfig,
                       "pathlib.Path | None: Path to the FDB configuration YAML.");

    py::class_<cdv::GribJumpExtractorDefinition, cdv::ExtractorDefinition>(
        extractor_type_ns, "GribJump",
        "Configuration for partial-field extraction via GribJump.\n\n"
        "GribJump avoids a full GRIB decode by jumping directly to the grid-point\n"
        "values inside each message. The implicit (grid-point) dimension can be\n"
        "split into equal-sized Zarr sub-chunks via *field_chunking*::\n\n"
        "    ExtractorType.GribJump(\n"
        "        field_chunking=FixedSizeChunking(1312),   # splits 5248 values into 4 chunks\n"
        "    )\n\n"
        "Args:\n"
        "    fdb_config (pathlib.Path | None): Path to the FDB configuration YAML.\n"
        "        ``None`` (default) uses the builder's FDB config.\n"
        "    gribjump_config (pathlib.Path | None): Path to the GribJump configuration YAML.\n"
        "        ``None`` (default) reads the ``GRIBJUMP_CONFIG_FILE`` environment variable.\n"
        "    field_chunking (WholeAxisChunking | SingleValueChunking | FixedSizeChunking | None):\n"
        "        How to sub-divide the implicit (grid-point) dimension into Zarr chunks.\n"
        "        ``None`` or :class:`WholeAxisChunking` (default) produces a single chunk\n"
        "        covering the full field. :class:`FixedSizeChunking` splits it into\n"
        "        equal-sized pieces.")
        .def(py::init([](std::optional<std::filesystem::path> fdbConfig,
                         std::optional<std::filesystem::path> gribjumpConfig,
                         std::optional<cdv::AxisDefinition::ChunkingType> fieldChunking) {
                 cdv::GribJumpExtractorDefinition definition{};
                 definition.fdbConfig = std::move(fdbConfig);
                 definition.gribjumpConfig = std::move(gribjumpConfig);
                 // The variant caster rejects anything that is not a chunking type, so no
                 // per-type checking is needed here.
                 if (fieldChunking.has_value()) {
                     definition.fieldChunking = std::move(*fieldChunking);
                 }
                 return definition;
             }),
             py::kw_only(), py::arg("fdb_config") = py::none(), py::arg("gribjump_config") = py::none(),
             py::arg("field_chunking") = py::none())
        .def_readwrite("fdb_config", &cdv::GribJumpExtractorDefinition::fdbConfig,
                       "pathlib.Path | None: Path to the FDB configuration YAML.")
        .def_readwrite("gribjump_config", &cdv::GribJumpExtractorDefinition::gribjumpConfig,
                       "pathlib.Path | None: Path to the GribJump configuration YAML.")
        .def_readwrite("field_chunking", &cdv::GribJumpExtractorDefinition::fieldChunking,
                       "WholeAxisChunking | SingleValueChunking | FixedSizeChunking: "
                       "Chunking of the implicit grid-point dimension.");

    py::class_<cdv::ChunkedDataViewBuilder>(
        m, "ChunkedDataViewBuilder",
        "Fluent builder that constructs a :class:`ChunkedDataView` from one or more\n"
        "MARS data parts.\n\n"
        "**Standard GRIB extraction** (full field, one chunk per field)::\n\n"
        "    builder = ChunkedDataViewBuilder(fdb_config_path)\n"
        "    builder.add_part(\n"
        "        'class=ea,type=an,...,param=167/131',\n"
        "        [AxisDefinition(keys=['date','time'], chunking=SingleValueChunking()),\n"
        "         AxisDefinition(keys=['param'],      chunking=SingleValueChunking())],\n"
        "        ExtractorType.Grib(),\n"
        "    )\n"
        "    view = builder.build()\n\n"
        "**GribJump extraction with implicit-axis chunking** — splits the grid-point\n"
        "dimension into fixed-size Zarr chunks::\n\n"
        "    builder = ChunkedDataViewBuilder(fdb_config_path)\n"
        "    builder.add_part(\n"
        "        'class=ea,type=an,...,param=167/131',\n"
        "        [AxisDefinition(keys=['date','time'], chunking=FixedSizeChunking(8)),\n"
        "         AxisDefinition(keys=['param'],      chunking=SingleValueChunking())],\n"
        "        ExtractorType.GribJump(field_chunking=FixedSizeChunking(1312)),\n"
        "    )\n"
        "    view = builder.build()\n\n"
        "When more than one part is added (e.g. surface and pressure-level fields),\n"
        "call :meth:`extend_on_axis` to declare which axis stitches the parts together.\n"
        "All parts must have identical extents on every other axis.")
        .def(py::init([](std::optional<std::filesystem::path> fdbConfigPath) {
                 return cdv::ChunkedDataViewBuilder(fdbConfigPath);
             }),
             py::arg("fdb_config_path") = py::none(),
             "Args:\n"
             "    fdb_config_path (pathlib.Path | None): Path to an FDB configuration file.\n"
             "        ``None`` (default) lets FDB resolve its configuration from the\n"
             "        environment (``FDB5_CONFIG`` / ``FDB_HOME``).")
        .def("add_part", &cdv::ChunkedDataViewBuilder::addPart, py::arg("mars_request"), py::arg("axes"),
             py::arg("extractor_type"), py::return_value_policy::reference_internal,
             "Register one part of the view.\n\n"
             "Each MARS keyword in *mars_request* that carries more than one value must\n"
             "appear in exactly one :class:`AxisDefinition` in *axes*. Multiple keywords\n"
             "may share one axis and are combined as a Cartesian product.\n\n"
             "Args:\n"
             "    mars_request (str):                         Comma-separated ``key=value[/value...]``\n"
             "                                                MARS request string, e.g.\n"
             "                                                ``'class=ea,param=167/131,date=20200101/20200102'``.\n"
             "    axes (list[AxisDefinition]):                 Axis definitions covering every\n"
             "                                                multi-valued key in the request.\n"
             "    extractor_type (ExtractorType.Grib | ExtractorType.GribJump):\n"
             "                                                Extractor configuration object.\n"
             "                                                ``ExtractorType.Grib`` reads the full GRIB field.\n"
             "                                                ``ExtractorType.GribJump`` supports splitting\n"
             "                                                the implicit grid-point axis into Zarr sub-chunks\n"
             "                                                (``field_chunking``).\n\n"
             "                                                The builder stores a copy, so one extractor can\n"
             "                                                be reused across parts and builders; defaults the\n"
             "                                                builder applies (e.g. its fdb_config) are not\n"
             "                                                written back into your object.\n\n"
             "Raises:\n"
             "    RuntimeError: If FDB cannot retrieve a sample field for the request, or if\n"
             "                  a returned paramId does not match the requested params.\n"
             "    TypeError: If *extractor* is not an ``ExtractorType.Grib`` or\n"
             "               ``ExtractorType.GribJump`` instance.")
        .def("extend_on_axis", &cdv::ChunkedDataViewBuilder::extendOnAxis, py::arg("axis"),
             py::return_value_policy::reference_internal,
             "Declare the axis index along which multiple parts are concatenated.\n\n"
             "Required when more than one part is added; ignored for single-part views.\n"
             "All parts must have identical extents on every axis except this one.\n\n"
             "Args:\n"
             "    axis (int): Zero-based index of the extension axis.\n\n"
             "Raises:\n"
             "    RuntimeError: If *axis* is out of range for the first part's axis list.")
        .def("fill_missing_value", &cdv::ChunkedDataViewBuilder::fillMissingValue, py::arg("value"),
             py::return_value_policy::reference_internal,
             "Set the fill value for array positions not covered by any data part.\n\n"
             "Args:\n"
             "    value (float): Fill value (default: ``float('nan')``).")
        .def("build", &cdv::ChunkedDataViewBuilder::build,
             "Validate all parts and assemble the :class:`ChunkedDataView`.\n\n"
             "Returns:\n"
             "    ChunkedDataView: The assembled view.\n\n"
             "Raises:\n"
             "    RuntimeError: On misconfiguration — no parts added, missing extension\n"
             "                  axis for multi-part views, incompatible part shapes, or\n"
             "                  inconsistent chunk sizes across parts.");
}
