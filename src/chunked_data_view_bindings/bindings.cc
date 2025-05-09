/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include <chunked_data_view/AxisDefinition.h>
#include <chunked_data_view/ChunkedDataView.h>
#include <chunked_data_view/ChunkedDataViewBuilder.h>
#include <chunked_data_view/Extractor.h>

#include <string>
#include <vector>

namespace py  = pybind11;
namespace cdv = chunked_data_view;

PYBIND11_MODULE(pychunked_data_view, m) {
    // Wrapping struct AxisDefinition
    py::class_<cdv::AxisDefinition>(m, "AxisDefinition")
        .def(py::init([](std::vector<std::string> keys, bool chunked) {
                 return cdv::AxisDefinition{std::move(keys), chunked};
             }),
             py::kw_only(), py::arg("keys"), py::arg("chunked"))
        .def_readwrite("keys", &cdv::AxisDefinition::keys)
        .def_readwrite("chunked", &cdv::AxisDefinition::chunked);

    // Wrapping interface ChunkedDataView
    py::class_<cdv::ChunkedDataView>(m, "ChunkedDataView")
        .def("at",
             [](cdv::ChunkedDataView* view, const cdv::ChunkedDataView::Index index) {
                 const auto& data = view->at(index);
                 py::array_t<double> arr(view->chunkShape());
                 ::memcpy(arr.mutable_data(), data.data(),
                          data.size() * sizeof(std::decay_t<decltype(data)>::value_type));
                 return arr;
             })
        .def("size", [](const cdv::ChunkedDataView* view) { return view->size(); })
        .def("chunk_shape", [](const cdv::ChunkedDataView* view) { return view->chunkShape(); })
        .def("shape", [](const cdv::ChunkedDataView* view) { return view->shape(); });

    py::enum_<cdv::ExtractorType>(m, "ExtractorType").value("GRIB", cdv::ExtractorType::GRIB);

    // Wrapper class ChunkedDataViewBuilder
    py::class_<cdv::ChunkedDataViewBuilder>(m, "ChunkedDataViewBuilder")
        .def(py::init([]() { return cdv::ChunkedDataViewBuilder(cdv::makeFdb()); }))
        .def(py::init([](const std::filesystem::path& fdbConfigPath) {
            return cdv::ChunkedDataViewBuilder(cdv::makeFdb(fdbConfigPath));
        }))
        .def("add_part",
             [](cdv::ChunkedDataViewBuilder& builder, std::string marsRequestKeyValues,
                std::vector<cdv::AxisDefinition> axes, const cdv::ExtractorType extractorType) {
                 builder.addPart(std::move(marsRequestKeyValues), std::move(axes), cdv::makeExtractor(extractorType));
             })
        .def("extend_on_axis", &cdv::ChunkedDataViewBuilder::extendOnAxis)
        .def("build", &cdv::ChunkedDataViewBuilder::build);
}
