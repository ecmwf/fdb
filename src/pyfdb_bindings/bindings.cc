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
#include <pybind11/native_enum.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include <cstddef>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/log/JSON.h"
#include "eckit/runtime/Main.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/BaseKey.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/IndexStats.h"
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
                               buf,               /* Name of parameter */
                               length);           /* Name of parameter */
    }

    long readinto(py::buffer buf) {
        PYBIND11_OVERRIDE_PURE(int,               /* Return type */
                               eckit::DataHandle, /* Parent class */
                               readinto,          /* Name of function in C++ (must match Python name) */
                               buf                /* Name of parameter */
        );                                        /* Name of parameter */
    }
};

PYBIND11_MODULE(pyfdb_bindings, m) {
    m.def("init_bindings", []() {
        const char* args[] = {"pyfdb", ""};
        eckit::Main::initialise(1, const_cast<char**>(args));
    });

    py::class_<fdb5::Config>(m, "Config")
        .def(py::init([]() { return fdb5::Config(); }))
        .def(py::init([](const std::string& config, const std::optional<std::string>& user_config) {
            if (user_config.has_value()) {
                return fdb5::Config(eckit::YAMLConfiguration(config), eckit::YAMLConfiguration(user_config.value()));
            }
            return fdb5::Config(eckit::YAMLConfiguration(config));
        }))
        .def("json",
             [](const fdb5::Config& config) {
                 std::stringstream buf;
                 auto json = eckit::JSON(buf);
                 json << config;
                 return buf.str();
             })
        .def("userConfig", [](const fdb5::Config& config) { return fdb5::Config(config.userConfig()); })
        .def("__repr__", [](const fdb5::Config& config) {
            std::stringstream buf;
            buf << config;
            return buf.str();
        });

    py::class_<eckit::DataHandle, PyDataHandle, py::smart_holder>(m, "DataHandle")
        .def(py::init())
        .def("open_for_read", [](eckit::DataHandle& data_handle) { data_handle.openForRead(); })
        .def("close", [](eckit::DataHandle& data_handle) { data_handle.close(); })
        .def("size", [](eckit::DataHandle& data_handle) { return static_cast<long long>(data_handle.size()); })
        .def("read",
             [](eckit::DataHandle& data_handle, py::buffer& buffer) {
                 py::buffer_info info = buffer.request();
                 return data_handle.read(info.ptr, info.size);
             })
        .def("__repr__", [](eckit::DataHandle& data_handle) {
            std::stringstream buf;
            buf << data_handle;
            return buf.str();
        });

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
        .def("__repr__", &mars::MarsRequest::asString);

    py::class_<fdb5::FDBToolRequest>(m, "FDBToolRequest")
        .def(py::init([](const mars::MarsRequest& mars_request, bool all, std::vector<std::string>& minimum_key_set) {
            return fdb5::FDBToolRequest(mars_request, all, minimum_key_set);
        }))
        .def("__repr__",
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
        .def("uri", [](const fdb5::ListElement& list_element) { return list_element.uri(); })
        .def(
            "data_handle", [](const fdb5::ListElement& list_element) { return list_element.location().dataHandle(); },
            py::return_value_policy::reference_internal)
        .def("__repr__", [](const fdb5::ListElement& list_element) {
            std::stringstream buf;
            list_element.print(buf, true, true, true, ",");
            return buf.str();
        });

    py::class_<fdb5::ListIterator>(m, "ListIterator")
        .def("__next__", [](fdb5::ListIterator& list_iterator) -> fdb5::ListElement {
            fdb5::ListElement result{};
            bool has_next = list_iterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    py::class_<fdb5::APIIterator<std::string>>(m, "StringApiIterator")
        .def("__next__", [](fdb5::APIIterator<std::string>& string_api_iterator) -> std::string {
            std::string result{};
            bool has_next = string_api_iterator.next(result);

            if (has_next) {
                return result;
            }

            throw py::stop_iteration();
        });

    py::class_<fdb5::ControlElement>(m, "ControlElement")
        .def(py::init())
        .def("location", [](fdb5::ControlElement& control_element) { return control_element.location; })
        .def("__repr__", [](fdb5::ControlElement& control_element) {
            std::stringstream buf{};
            buf << control_element.controlIdentifiers << ", ";
            buf << control_element.key << ", ";
            buf << control_element.location << ".";

            return buf.str();
        });

    py::class_<fdb5::APIIterator<fdb5::ControlElement>>(m, "ControlApiIterator")
        .def("__next__", [](fdb5::ControlIterator& status_iterator) -> fdb5::ControlElement {
            fdb5::ControlElement result{};
            bool has_next = status_iterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    py::class_<fdb5::FileCopy>(m, "FileCopy")
        .def(py::init())
        .def("__repr__",
             [](fdb5::FileCopy& file_copy_element) {
                 std::stringstream buf{};
                 buf << file_copy_element;
                 return buf.str();
             })
        .def("execute", &fdb5::FileCopy::execute)
        .def("cleanup", &fdb5::FileCopy::cleanup);

    py::class_<fdb5::APIIterator<fdb5::FileCopy>>(m, "FileCopyApiIterator")
        .def("__next__", [](fdb5::APIIterator<fdb5::FileCopy>& status_iterator) -> fdb5::FileCopy {
            fdb5::FileCopy result{};
            bool has_next = status_iterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    py::class_<fdb5::StatsElement>(m, "StatsElement")
        .def(py::init())
        .def("__repr__", [](const fdb5::StatsElement& stats_element) {
            std::stringstream buf{};
            buf << "Index Statistics: \n";
            stats_element.indexStatistics.report(buf);
            buf << "\n";
            buf << "DB Statistics: \n";
            stats_element.dbStatistics.report(buf);
            return buf.str();
        });

    py::class_<fdb5::APIIterator<fdb5::StatsElement>>(m, "StatsElementApiIterator")
        .def("__next__", [](fdb5::APIIterator<fdb5::StatsElement>& status_iterator) -> fdb5::StatsElement {
            fdb5::StatsElement result{};
            bool has_next = status_iterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    py::native_enum<fdb5::ControlAction>(m, "ControlAction", "enum.IntFlag")
        .value("NONE", fdb5::ControlAction::None)
        .value("DISABLE", fdb5::ControlAction::Disable)
        .value("ENABLE", fdb5::ControlAction::Enable)
        .finalize();

    py::native_enum<fdb5::ControlIdentifier>(m, "ControlIdentifier", "enum.IntFlag")
        .value("NONE", fdb5::ControlIdentifier::None)
        .value("LIST", fdb5::ControlIdentifier::List)
        .value("RETRIEVE", fdb5::ControlIdentifier::Retrieve)
        .value("ARCHIVE", fdb5::ControlIdentifier::Archive)
        .value("WIPE", fdb5::ControlIdentifier::Wipe)
        .value("UNIQUEROOT", fdb5::ControlIdentifier::UniqueRoot)
        .finalize();

    py::class_<fdb5::IndexAxis>(m, "IndexAxis")
        .def(py::init())
        .def("__repr__",
             [](const fdb5::IndexAxis& index_axis) {
                 std::stringstream buf{};
                 buf << index_axis;
                 return buf.str();
             })
        .def("__getitem__",
             [](const fdb5::IndexAxis& index_axis, const std::string& key) {
                 try {
                     const auto& values = index_axis.values(key);
                     return std::vector<std::string>{values.begin(), values.end()};
                 }
                 catch (const eckit::SeriousBug& serious_bug) {
                     std::stringstream buf;
                     buf << "Couldn't find key: " << key << " in IndexAxis.";
                     throw py::key_error(buf.str());
                 }
                 // INFO:: Clang isn't able to derive the correct type, so we are catching the base
                 catch (const eckit::Exception& serious_bug) {
                     std::stringstream buf;
                     buf << "Couldn't find key: " << key << " in IndexAxis.";
                     throw py::key_error(buf.str());
                 }
             })
        .def("keys",
             [](const fdb5::IndexAxis& index_axis) {
                 std::vector<std::string> result;
                 for (const auto& [key, _] : index_axis.map()) {
                     result.emplace_back(key);
                 }
                 return result;
             })
        .def("values",
             [](const fdb5::IndexAxis& index_axis) {
                 std::vector<std::vector<std::string>> result;
                 for (const auto& [_, values] : index_axis.map()) {
                     result.emplace_back(values.begin(), values.end());
                 }
                 return result;
             })
        .def("items",
             [](const fdb5::IndexAxis& index_axis) {
                 std::vector<std::pair<std::string, std::vector<std::string>>> result;
                 for (const auto& [key, values] : index_axis.map()) {
                     result.emplace_back(key, std::vector<std::string>{values.begin(), values.end()});
                 }
                 return result;
             })
        .def("__contains__",
             [](const fdb5::IndexAxis& index_axis, const std::string& key) {
                 const auto& map = index_axis.map();
                 const auto it   = map.find(key);

                 if (it == map.end()) {
                     return false;
                 }

                 return true;
             })
        .def("__len__", [](const fdb5::IndexAxis& index_axis) { return index_axis.map().size(); });


    py::class_<eckit::URI>(m, "URI")
        .def(py::init())
        .def(py::init([](const std::string& uri) { return eckit::URI(uri); }))
        .def(py::init([](const std::string& scheme, const std::string& path) {
            return eckit::URI(scheme, eckit::PathName(path));
        }))
        .def(py::init([](const std::string& scheme, const eckit::URI& uri) { return eckit::URI(scheme, uri); }))
        .def(py::init([](const std::string& scheme, const std::string& hostname, int port) {
            return eckit::URI(scheme, hostname, port);
        }))
        .def(py::init([](const std::string& scheme, const eckit::URI& uri, const std::string& hostname, int port) {
            return eckit::URI(scheme, uri, hostname, port);
        }))
        .def("name", &eckit::URI::name)
        .def("scheme", &eckit::URI::scheme)
        .def("user", &eckit::URI::user)
        .def("host", [](const eckit::URI& uri) { return uri.host(); })
        .def("port", [](const eckit::URI& uri) { return uri.port(); })
        .def("path", [](const eckit::URI& uri) { return uri.path().asString(); })
        .def("fragment", [](const eckit::URI& uri) { return uri.fragment(); })
        .def("rawString", &eckit::URI::asRawString)
        .def("__repr__", &eckit::URI::asString);


    py::class_<fdb5::FDB>(m, "FDB")
        .def(py::init([]() { return fdb5::FDB(); }))
        .def(py::init([](const fdb5::Config& conf) { return fdb5::FDB(conf); }))

        .def("archive", [](fdb5::FDB& fdb, const char* data, const size_t length) { return fdb.archive(data, length); })
        .def("archive", [](fdb5::FDB& fdb, const std::string& key, const char* data,
                           const size_t length) { return fdb.archive(fdb5::Key::parse(key), data, length); })
        .def("flush", &fdb5::FDB::flush)
        .def("retrieve", &fdb5::FDB::retrieve)
        .def("inspect", &fdb5::FDB::inspect)
        .def("list", &fdb5::FDB::list)
        .def("inspect", &fdb5::FDB::inspect)
        .def("dump", &fdb5::FDB::dump)
        .def("status", &fdb5::FDB::status)
        .def("wipe", &fdb5::FDB::wipe)
        .def("move", &fdb5::FDB::move)
        .def("purge", &fdb5::FDB::purge)
        .def("stats", [](fdb5::FDB& fdb, const fdb5::FDBToolRequest& tool_request) { return fdb.stats(tool_request); })
        .def("control",
             [](fdb5::FDB& fdb, const fdb5::FDBToolRequest& tool_request, const fdb5::ControlAction& control_action,
                const std::vector<fdb5::ControlIdentifier>& control_identifiers) {
                 auto interal_control_identifiers = fdb5::ControlIdentifiers();

                 for (const auto& control_identifier : control_identifiers) {
                     interal_control_identifiers |= control_identifier;
                 }
                 return fdb.control(tool_request, control_action, interal_control_identifiers);
             })
        .def("axes", &fdb5::FDB::axes)
        .def("enabled", &fdb5::FDB::enabled)
        .def("dirty", &fdb5::FDB::dirty)
        .def("config", [](const fdb5::FDB& fdb) { return fdb.config(); })
        .def("__repr__", [](const fdb5::FDB& fdb) {
            std::stringstream buf;
            buf << fdb;
            return buf.str();
        });
}
