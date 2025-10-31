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

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include <cstddef>
#include <map>
#include <string>
#include <vector>
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MultiHandle.h"
#include "eckit/runtime/Main.h"
#include "eckit/serialisation/Streamable.h"
#include "fdb5/api/FDB.h"
#include "fdb5/config/Config.h"
#include "metkit/mars/MarsRequest.h"

namespace py   = pybind11;
namespace mars = metkit::mars;

class PyDataHandle : public eckit::DataHandle, public py::trampoline_self_life_support {

    /* Trampoline (need one for each virtual function) */
    void print(std::ostream& s) const override {
        PYBIND11_OVERRIDE_PURE(void,              /* Return type */
                               eckit::DataHandle, /* Parent class */
                               print,             /* Name of function in C++ (must match Python name) */
        );
    }
    long read(void* buf, long length) override {
        PYBIND11_OVERRIDE_PURE(long,              /* Return type */
                               eckit::DataHandle, /* Parent class */
                               read,              /* Name of function in C++ (must match Python name) */
                               buf, length);
    }
};

PYBIND11_MODULE(pyfdb_bindings, m) {
    m.def("init_bindings", []() {
        const char* args[] = {"pyfdb", ""};
        eckit::Main::initialise(1, const_cast<char**>(args));
    });

    py::class_<fdb5::Config>(m, "Config")
        .def(py::init([]() { return fdb5::Config(); }))
        .def(py::init([](const std::string& config) { return fdb5::Config(eckit::YAMLConfiguration(config)); }));

    py::class_<eckit::DataHandle, PyDataHandle, py::smart_holder>(m, "DataHandle")
        .def(py::init())
        .def("read",
             [](eckit::DataHandle& data_handle, size_t len) {
                 std::vector<char> buf;
                 buf.resize(len);

                 data_handle.openForRead();
                 data_handle.read(buf.data(), len);

                 return py::bytes(buf.data(), buf.size());
             })
        .def("print", &eckit::DataHandle::print);

    py::class_<mars::MarsRequest>(m, "MarsRequest")
        .def(py::init([]() { return mars::MarsRequest(); }))
        .def(py::init([](const std::string& verb) { return mars::MarsRequest(verb); }))
        .def(py::init([](const std::string& verb, const std::map<std::string, std::string>& key_values) {
            return mars::MarsRequest(verb, key_values);
        }))
        .def("verb", [](mars::MarsRequest& mars_request) { return mars_request.verb(); })
        .def("key_values", [](mars::MarsRequest& mars_request) {
            std::map<std::string, std::vector<std::string>> result{};

            for (const auto& param : mars_request.parameters()) {
                const std::string& key = param.name();
                const auto& values     = param.values();
                result.emplace(key, values);
            }

            return result;
        });

    py::class_<fdb5::FDB>(m, "FDB")
        .def(py::init([]() { return fdb5::FDB(fdb5::Config()); }))
        .def(py::init([](const fdb5::Config& conf) { return fdb5::FDB(conf); }))
        .def("archive", [](fdb5::FDB& fdb, const char* data, const size_t length) { return fdb.archive(data, length); })
        .def("retrieve",
             [](fdb5::FDB& fdb, const mars::MarsRequest& mars_request) { return fdb.retrieve(mars_request); })
        .def("flush", [](fdb5::FDB& fdb) { return fdb.flush(); });
}
