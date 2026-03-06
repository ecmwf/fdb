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
#include <exception>
#include <map>
#include <memory>
#include <optional>
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
#include "eckit/value/Content.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/BaseKey.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/Key.h"
#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsRequest.h"

namespace py = pybind11;
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

    void close() override {
        PYBIND11_OVERRIDE(void,              /* Return type */
                          eckit::DataHandle, /* Parent class */
                          close,             /* Name of function in C++ (must match Python name) */
        );                                   /* Name of parameter */
    }
};

metkit::mars::MarsRequest mars_requestfrom_map(const std::map<std::string, std::vector<std::string>>& map) {
    eckit::ValueMap value_map;

    for (const auto& pair : map) {
        eckit::ValueList value_list;
        for (const auto& value : pair.second) {
            value_list.emplace_back(value);
        }
        value_map.emplace(eckit::Value(pair.first), value_list);
    }

    // Expand the mars request
    auto mars_request = metkit::mars::MarsRequest("retrieve", value_map);

    const bool inherit = false;
    const bool strict = true;
    metkit::mars::MarsExpansion expand(inherit, strict);

    return expand.expand(mars_request);
}

PYBIND11_MODULE(pyfdb_bindings, m) {
    m.def("init_bindings", []() {
        const char* args[] = {"pyfdb", ""};
        eckit::Main::initialise(1, const_cast<char**>(args));
    });

    //--------------------------------------------------
    // @brief Enums
    //--------------------------------------------------

    py::native_enum<fdb5::ControlAction>(m, "ControlAction", "enum.Enum")
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


    //--------------------------------------------------
    // @brief Utility classes
    //--------------------------------------------------

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
        .def("open", [](eckit::DataHandle& data_handle) { data_handle.openForRead(); })
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

    py::class_<fdb5::FDBToolRequest>(m, "FDBToolRequest")
        .def(py::init([](const std::map<std::string, std::vector<std::string>>& selection, bool all,
                         std::vector<std::string>& minimum_key_set) {
            return fdb5::FDBToolRequest(mars_requestfrom_map(selection), all, minimum_key_set);
        }))
        .def("__repr__",
             [](const fdb5::FDBToolRequest& tool_request) {
                 std::stringstream buf;
                 tool_request.print(buf);
                 return buf.str();
             })
        .def("mars_request", [](const fdb5::FDBToolRequest& tool_request) { return tool_request.request(); });

    py::class_<fdb5::IndexAxis>(m, "IndexAxis")
        .def(py::init())
        .def(py::init([](const std::map<std::string, std::vector<std::string>>& map) {
            fdb5::IndexAxis result{};
            for (const auto& [key, values] : map) {
                result.insert(key, values);
            }
            return result;
        }))
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
                 const auto it = map.find(key);

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

    //--------------------------------------------------
    // @brief Element class Enums
    //--------------------------------------------------

    py::native_enum<fdb5::WipeElementType>(m, "WipeElementType", "enum.Enum")
        .value("ERROR", fdb5::WipeElementType::ERROR)
        .value("CATALOGUE_INFO", fdb5::WipeElementType::CATALOGUE_INFO)
        .value("CATALOGUE", fdb5::WipeElementType::CATALOGUE)
        .value("CATALOGUE_INDEX", fdb5::WipeElementType::CATALOGUE_INDEX)
        .value("CATALOGUE_SAFE", fdb5::WipeElementType::CATALOGUE_SAFE)
        .value("CATALOGUE_CONTROL", fdb5::WipeElementType::CATALOGUE_CONTROL)
        .value("STORE", fdb5::WipeElementType::STORE)
        .value("STORE_AUX", fdb5::WipeElementType::STORE_AUX)
        .value("STORE_SAFE", fdb5::WipeElementType::STORE_SAFE)
        .value("UNKNOWN", fdb5::WipeElementType::UNKNOWN)
        .finalize();

    //--------------------------------------------------
    // @brief Element classes
    //--------------------------------------------------

    py::class_<fdb5::ListElement>(m, "ListElement")
        .def(py::init())
        .def(py::init([](const std::string& key, const std::time_t& timestamp) {
            return fdb5::ListElement(fdb5::Key::parse(key), timestamp);
        }))
        .def("has_location", &fdb5::ListElement::hasLocation)
        .def("offset", [](const fdb5::ListElement& list_element) -> long long { return list_element.offset(); })
        .def("length", [](const fdb5::ListElement& list_element) -> long long { return list_element.length(); })
        .def("uri",
             [](const fdb5::ListElement& list_element) -> std::optional<eckit::URI> {
                 try {
                     return list_element.location().uri();
                 }
                 catch (std::exception& exception) {
                     return std::nullopt;
                 };
             })
        .def(
            "data_handle",
            [](const fdb5::ListElement& list_element) -> std::optional<std::shared_ptr<eckit::DataHandle>> {
                try {
                    return std::shared_ptr<eckit::DataHandle>(list_element.location().dataHandle());
                }
                catch (std::exception& exception) {
                    return std::nullopt;
                };
            },
            py::return_value_policy::reference_internal)
        .def("__repr__", [](const fdb5::ListElement& list_element) {
            std::stringstream buf;
            list_element.print(buf, true, true, true, ",");
            return buf.str();
        });

    py::class_<fdb5::ControlElement>(m, "ControlElement")
        .def(py::init())
        .def("location", [](fdb5::ControlElement& control_element) { return control_element.location; })
        .def("controlIdentifiers",
             [](fdb5::ControlElement& control_element) {
                 std::vector<fdb5::ControlIdentifier> result;
                 for (const auto& control_identifier : control_element.controlIdentifiers) {
                     result.emplace_back(control_identifier);
                 }
                 return result;
             })
        .def("key",
             [](fdb5::ControlElement& control_element) {
                 std::map<std::string, std::vector<std::string>> key_value_map;
                 for (const auto& key : control_element.key.names()) {
                     key_value_map.emplace(key, std::vector<std::string>{control_element.key.get(key)});
                 }
                 return key_value_map;
             })
        .def("__repr__", [](fdb5::ControlElement& control_element) {
            std::stringstream buf{};
            buf << control_element.controlIdentifiers << ", ";
            buf << control_element.key << ", ";
            buf << control_element.location << ".";

            return buf.str();
        });

    py::class_<fdb5::WipeElement>(m, "WipeElement")
        .def(py::init())
        .def("type", &fdb5::WipeElement::type)
        .def("msg", &fdb5::WipeElement::msg)
        .def("uris", &fdb5::WipeElement::uris)
        .def("__repr__", [](fdb5::WipeElement& wipe_element) {
            std::stringstream buf{};
            buf << wipe_element;
            return buf.str();
        });

    py::class_<fdb5::PurgeElement>(m, "PurgeElement")
        .def(py::init())
        .def("__repr__", [](fdb5::PurgeElement& purge_element) { return purge_element; });

    py::class_<fdb5::StatsElement>(m, "StatsElement")
        .def(py::init())
        .def("index_statistics",
             [](fdb5::StatsElement& stats_element) {
                 std::stringstream buf{};
                 stats_element.indexStatistics.report(buf);
                 return buf.str();
             })
        .def("db_statistics",
             [](const fdb5::StatsElement& stats_element) {
                 std::stringstream buf{};
                 stats_element.dbStatistics.report(buf);
                 return buf.str();
             })
        .def("__repr__", [](const fdb5::StatsElement& stats_element) {
            std::stringstream buf{};
            buf << "Index Statistics: \n";
            stats_element.indexStatistics.report(buf);
            buf << "\n";
            buf << "DB Statistics: \n";
            stats_element.dbStatistics.report(buf);
            return buf.str();
        });


    //--------------------------------------------------
    // @brief Iterator classes
    //--------------------------------------------------

    py::class_<fdb5::ListIterator>(m, "ListIterator")
        .def("__iter__", [](fdb5::ListIterator& self) -> fdb5::ListIterator& { return self; })
        .def("__next__", [](fdb5::ListIterator& list_iterator) -> fdb5::ListElement {
            fdb5::ListElement result{};
            bool has_next = list_iterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    py::class_<fdb5::WipeIterator>(m, "WipeIterator")
        .def("__iter__", [](fdb5::WipeIterator& self) -> fdb5::WipeIterator& { return self; })
        .def("__next__", [](fdb5::WipeIterator& wipe_iterator) -> fdb5::WipeElement {
            fdb5::WipeElement result{};
            bool has_next = wipe_iterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    py::class_<fdb5::PurgeIterator>(m, "PurgeIterator")
        .def("__iter__", [](fdb5::PurgeIterator& self) -> fdb5::PurgeIterator& { return self; })
        .def("__next__", [](fdb5::PurgeIterator& purgeiterator) -> fdb5::PurgeElement {
            fdb5::PurgeElement result{};
            bool has_next = purgeiterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    py::class_<fdb5::ControlIterator>(m, "ControlIterator")
        .def("__iter__", [](fdb5::ControlIterator& self) -> fdb5::ControlIterator& { return self; })
        .def("__next__", [](fdb5::ControlIterator& status_iterator) -> fdb5::ControlElement {
            fdb5::ControlElement result{};
            bool has_next = status_iterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    py::class_<fdb5::StatsIterator>(m, "StatsIterator")
        .def("__iter__", [](fdb5::StatsIterator& self) -> fdb5::StatsIterator& { return self; })
        .def("__next__", [](fdb5::StatsIterator& status_iterator) -> fdb5::StatsElement {
            fdb5::StatsElement result{};
            bool has_next = status_iterator.next(result);
            if (has_next) {
                return result;
            }
            throw py::stop_iteration();
        });

    //--------------------------------------------------
    // @brief FDB class
    //--------------------------------------------------

    py::class_<fdb5::FDB, py::smart_holder>(m, "FDB")
        .def(py::init())
        .def(py::init<const fdb5::Config&>())

        .def("archive", [](fdb5::FDB& fdb, const char* data, const size_t length) { return fdb.archive(data, length); })
        .def("archive",
             [](fdb5::FDB& fdb, const std::vector<std::tuple<std::string, std::string>>& key, const char* data,
                const size_t length) {
                 fdb5::Key mapped_key{};
                 for (const auto& [k, v] : key) {
                     mapped_key.push(k, v);
                 }
                 return fdb.archive(mapped_key, data, length);
             })
        .def("flush", &fdb5::FDB::flush)
        .def("retrieve",
             [](fdb5::FDB& fdb, const std::map<std::string, std::vector<std::string>>& selection) {
                 return fdb.retrieve(mars_requestfrom_map(selection));
             })
        .def("inspect",
             [](fdb5::FDB& fdb, const std::map<std::string, std::vector<std::string>>& selection) {
                 return fdb.inspect(mars_requestfrom_map(selection));
             })
        .def("list", [](fdb5::FDB& fdb, const fdb5::FDBToolRequest& tool_request, bool deduplicate,
                        int level) { return fdb.list(tool_request, deduplicate, level); })
        .def("inspect", &fdb5::FDB::inspect)
        .def("dump", &fdb5::FDB::dump)
        .def("status", &fdb5::FDB::status)
        .def("wipe", &fdb5::FDB::wipe)
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
