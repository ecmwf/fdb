// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <memory>
#include <sstream>
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
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/GribExtractor.h"
#include "chunked_data_view/LibChunkedDataView.h"
#include "chunked_data_view/exception/UnknownExtractorException.h"

namespace py = pybind11;
namespace cdv = chunked_data_view;

PYBIND11_MODULE(chunked_data_view_bindings, m) {
    m.doc() =
        "Low-level pybind11 bindings for the chunked_data_view C++ library.\n\n"
        "This module exposes the core types needed to build a Zarr-compatible N-dimensional\n"
        "view over FDB data. It is not part of the public API; users should import from\n"
        "``pychunked_data_view`` or ``z3fdb`` instead.\n\n"
        "Typical call sequence::\n\n"
        "    init_bindings()\n"
        "    builder = ChunkedDataViewBuilder(fdb_config_path)\n"
        "    builder.add_part(mars_request_string, [AxisDefinition(...)], ExtractorType.GRIB)\n"
        "    view = builder.build()\n"
        "    chunk = view.at([0, 1, 0])  # numpy array of float32\n";

    m.def(
        "init_bindings", []() { cdv::init_eckit_main(); },
        "Initialise the eckit main singleton.\n\n"
        "Must be called exactly once before any other function or class in this module is\n"
        "used. Subsequent calls are safe but have no effect.");

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
        "The chunk size must be a valid trailing-product divisor of the combined axis\n"
        "size; see ``AxisMapper::chunkSizeCheck`` for the exact rule. Use this when\n"
        "a tuned chunk size is needed to balance read amplification against request\n"
        "count (e.g. grouping every 3 time steps into one chunk).\n\n"
        "Args:\n"
        "    chunk_size: Number of axis elements per chunk.")
        .def(py::init<>([](size_t& chunkExtension) {
                 return cdv::AxisDefinition::FixedSizeChunking{.chunkSize = chunkExtension};
             }),
             py::arg("chunk_size"))
        .def(
            "chunk_shape",
            [](const cdv::AxisDefinition::FixedSizeChunking* fixedSizeChunking) {
                return fixedSizeChunking->chunkSize;
            },
            "int: Number of axis elements per chunk.");

    axis_definition
        .def(py::init([](std::vector<std::string> keys, cdv::AxisDefinition::ChunkingType chunking) {
                 return cdv::AxisDefinition{std::move(keys), chunking};
             }),
             py::kw_only(), py::arg("keys"), py::arg("chunking"))
        .def(py::init([](std::vector<std::string> keys, cdv::AxisDefinition::FixedSizeChunking chunking) {
                 return cdv::AxisDefinition{std::move(keys), chunking};
             }),
             py::kw_only(), py::arg("keys"), py::arg("chunking"))
        .def(py::init([](std::vector<std::string> keys, cdv::AxisDefinition::WholeAxisChunking chunking) {
                 return cdv::AxisDefinition{std::move(keys), chunking};
             }),
             py::kw_only(), py::arg("keys"), py::arg("chunking"))
        .def_readwrite("keys", &cdv::AxisDefinition::keys, "list[str]: Ordered MARS keyword names that form this axis.")
        .def_readwrite("chunking", &cdv::AxisDefinition::chunking,
                       "WholeAxisChunking | SingleValueChunking | FixedSizeChunking: "
                       "Chunking strategy applied to this axis.");

    // ChunkedDataView
    py::class_<cdv::ChunkedDataView>(m, "ChunkedDataView",
                                     "Read-only N-dimensional chunked array backed by FDB field data.\n\n"
                                     "Coordinates are expressed as chunk indices: one integer per dimension\n"
                                     "identifying a chunk's position in the chunk grid. Fetching a chunk\n"
                                     "triggers one or more FDB retrievals and assembles the field values into\n"
                                     "a contiguous ``float32`` numpy array.\n\n"
                                     "The last dimension is the implicit grid-point dimension (number of values\n"
                                     "per GRIB message). It is never chunked and is always returned in full.\n\n"
                                     "Instances are created exclusively by :meth:`ChunkedDataViewBuilder.build`.")
        .def(
            "at",
            [](cdv::ChunkedDataView* view, const cdv::ChunkedDataView::Index index) {
                const auto len = view->countChunkValues();
                py::array_t<float> arr(len);
                float* p = arr.mutable_data();
                view->at(index, p, len);
                return arr;
            },
            py::arg("index"),
            "Return the data for the chunk at *index* as a 1-D ``float32`` numpy array.\n\n"
            "Args:\n"
            "    index (list[int]): Chunk-grid coordinates, one entry per dimension\n"
            "                       (including the implicit grid-point dimension whose\n"
            "                       index must always be 0).\n\n"
            "Returns:\n"
            "    numpy.ndarray: 1-D float32 array of length ``chunk_shape()[-1]``\n"
            "                   containing the field values for this chunk.\n\n"
            "Raises:\n"
            "    RuntimeError: If the FDB retrieval fails or the index is out of bounds.")
        .def(
            "chunk_shape", [](const cdv::ChunkedDataView* view) { return view->chunkShape(); },
            "tuple[int, ...]: Number of elements per chunk in each dimension (Zarr chunk shape).")
        .def(
            "chunks", [](const cdv::ChunkedDataView* view) { return view->chunks(); },
            "tuple[int, ...]: Number of chunks along each dimension of the chunk grid.")
        .def(
            "shape", [](const cdv::ChunkedDataView* view) { return view->shape(); },
            "tuple[int, ...]: Total number of elements along each dimension of the full array.")
        .def(
            "fill_missing_value", [](const cdv::ChunkedDataView* view) { return view->fillMissingValue(); },
            "float: Value written for array positions not covered by any data part\n"
            "       (default: ``float('nan')``).");

    // ExtractorType
    py::enum_<cdv::ExtractorType>(m, "ExtractorType",
                                  "Selects the data format expected in FDB.\n\n"
                                  "Passed to :meth:`ChunkedDataViewBuilder.add_part` to control which\n"
                                  ":class:`Extractor` implementation is instantiated for a data part.")
        .value("GRIB", cdv::ExtractorType::GRIB,
               "Data is stored as GRIB messages. The extractor reads each message,\n"
               "validates the returned paramIds against the request, and copies the\n"
               "``values`` array into the chunk buffer.");

    // ChunkedDataViewBuilder
    py::class_<cdv::ChunkedDataViewBuilder>(
        m, "ChunkedDataViewBuilder",
        "Fluent builder that constructs a :class:`ChunkedDataView` from one or more\n"
        "MARS data parts.\n\n"
        "Usage::\n\n"
        "    builder = ChunkedDataViewBuilder(fdb_config_path)\n"
        "    builder.add_part(\n"
        "        'class=ea,type=an,...,param=167/131',\n"
        "        [AxisDefinition(keys=['date','time'], chunking=SingleValueChunking()),\n"
        "         AxisDefinition(keys=['param'],      chunking=SingleValueChunking())],\n"
        "        ExtractorType.GRIB,\n"
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
        .def(
            "add_part",
            [](cdv::ChunkedDataViewBuilder& builder, std::string marsRequestKeyValues,
               std::vector<cdv::AxisDefinition> axes, const cdv::ExtractorType extractorType) {
                switch (extractorType) {
                    case chunked_data_view::ExtractorType::GRIB: {
                        auto fdb = cdv::makeFdb(builder.getFdbConfigPath());
                        auto extractor = std::make_shared<chunked_data_view::GribExtractor>(
                            chunked_data_view::GribExtractor(std::move(fdb)));
                        builder.addPart(std::move(marsRequestKeyValues), std::move(axes), std::move(extractor));
                        break;
                    }
                    default:
                        std::stringstream buf;
                        buf << "ChunkedDataViewBuidler::add_part: Unknown Extractor of type " << std::endl;
                        throw cdv::UnknownExtractorException(buf.str());
                }
            },
            py::arg("mars_request"), py::arg("axes"), py::arg("extractor_type"),
            "Register one data region (part) of the view.\n\n"
            "Each MARS keyword in *mars_request* that carries more than one value must\n"
            "appear in exactly one :class:`AxisDefinition` in *axes*. Multiple keywords\n"
            "may share one axis and are combined as a Cartesian product.\n\n"
            "Args:\n"
            "    mars_request  (str):              Comma-separated ``key=value[/value...]``\n"
            "                                      MARS request string, e.g.\n"
            "                                      ``'class=ea,param=167/131,date=20200101/20200102'``.\n"
            "    axes (list[AxisDefinition]):       Axis definitions covering every\n"
            "                                      multi-valued key in the request.\n"
            "    extractor_type (ExtractorType):   Format of the data in FDB.\n\n"
            "Raises:\n"
            "    RuntimeError: If FDB cannot retrieve a sample field for the request, or if\n"
            "                  a returned paramId does not match the requested params (which\n"
            "                  indicates unsupported on-the-fly field derivation).")
        .def("extend_on_axis", &cdv::ChunkedDataViewBuilder::extendOnAxis, py::arg("axis"),
             "Declare the axis index along which multiple parts are concatenated.\n\n"
             "Required when more than one part is added; ignored for single-part views.\n"
             "All parts must have identical extents on every axis except this one.\n\n"
             "Args:\n"
             "    axis (int): Zero-based index of the extension axis.\n\n"
             "Raises:\n"
             "    RuntimeError: If *axis* is out of range for the first part's axis list.")
        .def("fill_missing_value", &cdv::ChunkedDataViewBuilder::fillMissingValue, py::arg("value"),
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
