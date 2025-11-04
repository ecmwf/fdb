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

#include <pybind11/iostream.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include <cstddef>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <vector>
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/io/DataHandle.h"
#include "eckit/runtime/Main.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/BaseKey.h"
#include "fdb5/database/Key.h"
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
        .def("key_values",
             [](mars::MarsRequest& mars_request) {
                 std::map<std::string, std::vector<std::string>> result{};

                 for (const auto& param : mars_request.parameters()) {
                     const std::string& key = param.name();
                     const auto& values     = param.values();
                     result.emplace(key, values);
                 }

                 return result;
             })
        .def("empty", &mars::MarsRequest::empty)
        .def("__len__", [](const mars::MarsRequest& mars_request) { return mars_request.params().size(); })
        .def("__str__", &mars::MarsRequest::asString);

    py::class_<fdb5::FDBToolRequest>(m, "FDBToolRequest")
        .def(py::init([](const mars::MarsRequest& mars_request, bool all, std::vector<std::string>& minimum_key_set) {
            return fdb5::FDBToolRequest(mars_request, all, minimum_key_set);
        }))
        .def("__str__",
             [](const fdb5::FDBToolRequest& tool_request) {
                 std::stringstream buf;
                 tool_request.print(buf);
                 return buf.str();
             })
        .def("mars_request", [](const fdb5::FDBToolRequest& tool_request) { return tool_request.request(); });

    py::class_<fdb5::ListElement>(m, "ListElement")
        .def(py::init())
        .def(py::init([](const std::string& key, const std::time_t& timestamp) {
            return fdb5::ListElement(fdb5::Key::parse(key), timestamp);
        }))
        .def("__str__", [](const fdb5::ListElement& list_element) {
            std::stringstream buf;
            list_element.print(buf, true, true, true, ",");
            return buf.str();
        });

    py::class_<fdb5::ListIterator>(m, "ListIterator")
        .def("next", [](fdb5::ListIterator& list_iterator) -> std::optional<fdb5::ListElement> {
            fdb5::ListElement result{};
            bool has_next = list_iterator.next(result);
            if (has_next) {
                return std::make_optional(result);
            }
            else {
                return std::nullopt;
            }
        });

    py::class_<fdb5::WipeElement>(m, "WipeElement")
        .def(py::init())
        .def(py::init([](const std::string& element) { return fdb5::WipeElement(element); }))
        .def("__str__", [](const fdb5::WipeElement& wipe_element) { return wipe_element; });


    py::class_<fdb5::WipeIterator>(m, "WipeIterator")
        .def("next", [](fdb5::WipeIterator& wipe_iterator) -> std::optional<fdb5::WipeElement> {
            fdb5::WipeElement result{};
            bool has_next = wipe_iterator.next(result);

            if (has_next) {
                return std::make_optional(result);
            }
            return std::nullopt;
        });

    // py::class_<fdb5::Key>(m, "Key")
    //     .def(py::init())
    //     .def(py::init(
    //         [](std::vector<std::pair<const std::string, std::string>>& key_value) { return fdb5::Key(key_value); }))
    //     .def("__str__", [](const fdb5::WipeElement& wipe_element) { return wipe_element; });
    //

    py::class_<fdb5::FDB>(m, "FDB")
        .def(py::init([]() { return fdb5::FDB(fdb5::Config()); }))
        .def(py::init([](const fdb5::Config& conf) { return fdb5::FDB(conf); }))
        .def("archive", [](fdb5::FDB& fdb, const char* data, const size_t length) { return fdb.archive(data, length); })
        .def("archive", [](fdb5::FDB& fdb, const std::string& key, const char* data,
                           const size_t length) { return fdb.archive(fdb5::Key::parse(key), data, length); })
        .def("archive", [](fdb5::FDB& fdb, const mars::MarsRequest& mars_request,
                           eckit::DataHandle& data_handle) { return fdb.archive(mars_request, data_handle); })
        .def("reindex", [](fdb5::FDB& fdb, fdb5::Key& key,
                           fdb5::FieldLocation& field_location) { return fdb.reindex(key, field_location); })
        .def("retrieve",
             [](fdb5::FDB& fdb, const mars::MarsRequest& mars_request) { return fdb.retrieve(mars_request); })
        .def("list", [](fdb5::FDB& fdb, const fdb5::FDBToolRequest& tool_request,
                        size_t level) { return fdb.list(tool_request, false, level); })
        .def("list_no_duplicates", [](fdb5::FDB& fdb, const fdb5::FDBToolRequest& tool_request,
                                      size_t level) { return fdb.list(tool_request, true, level); })
        .def("wipe", &fdb5::FDB::wipe)
        .def("flush", &fdb5::FDB::flush);
}
