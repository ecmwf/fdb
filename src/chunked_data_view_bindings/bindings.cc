/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

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
    m.def("init_bindings", []() { cdv::init_eckit_main(); });
    // Wrapping struct AxisDefinition
    auto axis_definition =
        py::class_<cdv::AxisDefinition>(m, "AxisDefinition")
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
            .def_readwrite("keys", &cdv::AxisDefinition::keys)
            .def_readwrite("chunking", &cdv::AxisDefinition::chunking);

    // Wrapping AxisDefinition::WholeAxisChunking as nested class of AxisDefinition
    py::class_<cdv::AxisDefinition::WholeAxisChunking>(axis_definition, "WholeAxisChunking").def(py::init<>([]() {
        return cdv::AxisDefinition::WholeAxisChunking{};
    }));

    // Wrapping AxisDefinition::SingleValueChunking as nested class of Axisdefinition
    py::class_<cdv::AxisDefinition::SingleValueChunking>(axis_definition, "SingleValueChunking").def(py::init<>([]() {
        return cdv::AxisDefinition::SingleValueChunking{};
    }));

    // Wrapping AxisDefinition::FixedSizeChunking as nested class of AxisDefinition
    py::class_<cdv::AxisDefinition::FixedSizeChunking>(axis_definition, "FixedSizeChunking")
        .def(py::init<>(
            [](size_t& chunkExtension) { return cdv::AxisDefinition::FixedSizeChunking{.chunkSize = chunkExtension}; }))
        .def("chunk_shape", [](const cdv::AxisDefinition::FixedSizeChunking* fixedSizeChunking) {
            return fixedSizeChunking->chunkSize;
        });

    // Wrapping interface ChunkedDataView
    py::class_<cdv::ChunkedDataView>(m, "ChunkedDataView")
        .def("at",
             [](cdv::ChunkedDataView* view, const cdv::ChunkedDataView::Index index) {
                 const auto len = view->countChunkValues();
                 py::array_t<float> arr(len);
                 float* p = arr.mutable_data();
                 view->at(index, p, len);

                 return arr;
             })
        .def("chunk_shape", [](const cdv::ChunkedDataView* view) { return view->chunkShape(); })
        .def("chunks", [](const cdv::ChunkedDataView* view) { return view->chunks(); })
        .def("shape", [](const cdv::ChunkedDataView* view) { return view->shape(); })
        .def("fill_missing_value", [](const cdv::ChunkedDataView* view) { return view->fillMissingValue(); });

    py::enum_<cdv::ExtractorType>(m, "ExtractorType").value("GRIB", cdv::ExtractorType::GRIB);

    // Wrapper class ChunkedDataViewBuilder
    py::class_<cdv::ChunkedDataViewBuilder>(m, "ChunkedDataViewBuilder")
        .def(py::init([](std::optional<std::filesystem::path> fdbConfigPath) {
            return cdv::ChunkedDataViewBuilder(fdbConfigPath);
        }))
        .def("add_part",
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
             })
        .def("extend_on_axis", &cdv::ChunkedDataViewBuilder::extendOnAxis)
        .def("fill_missing_value", &cdv::ChunkedDataViewBuilder::fillMissingValue)
        .def("build", &cdv::ChunkedDataViewBuilder::build);
}
