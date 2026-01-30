/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <chunked_data_view/AxisDefinition.h>
#include <chunked_data_view/ChunkedDataView.h>
#include <chunked_data_view/ChunkedDataViewBuilder.h>
#include <chunked_data_view/Extractor.h>
#include <chunked_data_view/LibChunkedDataView.h>
#include <chunked_data_view/NDCoord.h>

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include <string>
#include <vector>

namespace py  = pybind11;
namespace cdv = chunked_data_view;

PYBIND11_MODULE(chunked_data_view_bindings, m) {
    m.def("init_bindings", []() { cdv::init_eckit_main(); });
    // Wrapping struct AxisDefinition
    auto axis_definition =
        py::class_<cdv::AxisDefinition>(m, "AxisDefinition")
            .def(py::init([](std::vector<std::string> keys, cdv::AxisDefinition::ChunkingType chunking) {
                     return cdv::AxisDefinition{std::move(keys), chunking};
                 }),
                 py::kw_only(), py::arg("keys"), py::arg("chunking"))
            .def_readwrite("keys", &cdv::AxisDefinition::keys)
            .def_readwrite("chunking", &cdv::AxisDefinition::chunking);

    // Wrapping AxisDefinition::NoChunking as nested class of Axisdefinition
    py::class_<cdv::AxisDefinition::NoChunking>(axis_definition, "NoChunking").def(py::init<>());

    // Wrapping AxisDefinition::IndividualChunking as nested class of Axisdefinition
    py::class_<cdv::AxisDefinition::IndividualChunking>(axis_definition, "IndividualChunking").def(py::init<>());

    // Wrapping interface ChunkedDataView
    py::class_<cdv::ChunkedDataView>(m, "ChunkedDataView")
        .def("at",
             [](cdv::ChunkedDataView* view, const std::vector<int32_t> index) {
                 const auto len = view->countChunkValues();
                 py::array_t<float> arr(len);
                 float* p = arr.mutable_data();
                 view->at(cdv::ChunkIndex(std::move(index)), p, len);
                 return arr;
             })
        .def("chunk_shape", [](const cdv::ChunkedDataView* view) { return view->chunkShape().data; })
        .def("chunks", [](const cdv::ChunkedDataView* view) { return view->chunks().data; })
        .def("shape", [](const cdv::ChunkedDataView* view) { return view->shape().data; })
        .def("datum_size", [](const cdv::ChunkedDataView* view){ return view->datumSize();});

    py::enum_<cdv::ExtractorType>(m, "ExtractorType").value("GRIB", cdv::ExtractorType::GRIB);

    // Wrapper class ChunkedDataViewBuilder
    py::class_<cdv::ChunkedDataViewBuilder>(m, "ChunkedDataViewBuilder")
        .def(py::init([](std::optional<std::filesystem::path> fdbConfigPath) {
            if (fdbConfigPath) {
                return cdv::ChunkedDataViewBuilder(cdv::makeFdb(*fdbConfigPath));
            }
            else {
                return cdv::ChunkedDataViewBuilder(cdv::makeFdb());
            }
        }))
        .def("add_part",
             [](cdv::ChunkedDataViewBuilder& builder, std::string marsRequestKeyValues,
                std::vector<cdv::AxisDefinition> axes, const cdv::ExtractorType extractorType) {
                 builder.addPart(std::move(marsRequestKeyValues), std::move(axes), cdv::makeExtractor(extractorType));
             })
        .def("extend_on_axis", &cdv::ChunkedDataViewBuilder::extendOnAxis)
        .def("build", &cdv::ChunkedDataViewBuilder::build);
}
