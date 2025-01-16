#include <pybind11/pybind11.h>
#include <pybind11/iostream.h>
#include <pybind11/stl.h>
#include <cstddef>

#include "eckit/config/Configuration.h"
#include "eckit/eckit_version.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/TransferWatcher.h"
#include "eckit/message/Message.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/LocalFDB.h"
#include "fdb5/api/fdb_c.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Inspector.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Key.h"
#include "metkit/mars/Parameter.h"

namespace py = pybind11;


PYBIND11_MODULE(_core, module) {
    module.doc() = "pyfdb c++ api"; // optional module docstring

    py::class_<eckit::PathName>(module, "PathName")
       .def(py::init<const std::string&, bool>())
       .def("asString", &eckit::PathName::asString);

    py::class_<eckit::LocalConfiguration>(module, "LocalConfiguration")
        .def(py::init<const eckit::Configuration&, const eckit::PathName&>());
        .def(py::init([](const std::unordered_map<std::string, std::string> key_value_map){
                    auto result_map = eckit::LocalConfiguration();
                    for()
                    }))

    py::class_<fdb5::Config>(module, "Config")
        .def(py::init<>())
        .def(py::init<const eckit::Configuration&, const eckit::Configuration&>());

    py::class_<metkit::mars::MarsRequest>(module, "MarsRequest")
        .def(py::init<>())
        .def(py::init<const std::string&>())
        .def(py::init<const std::string&, const std::map<std::string, std::string>&>())
        .def("asString", &metkit::mars::MarsRequest::asString);

    py::class_<fdb5::FDBToolRequest>(module, "FDBToolRequest")
        .def(py::init<const metkit::mars::MarsRequest&, bool, const std::vector<std::string>&>());

    py::class_<fdb5::ListElement>(module, "ListElement")
        .def(py::init<>())
        .def("__str__", [](fdb5::ListElement& el){
                std::stringstream buffer;
                buffer << el;
                return buffer.str();
                });

    py::class_<fdb5::APIIterator<fdb5::ListElement>>(module, "APIIterator");
    py::class_<fdb5::ListIterator, fdb5::APIIterator<fdb5::ListElement>>(module, "ListIterator")
        .def("__iter__", [](fdb5::ListIterator &it) -> fdb5::ListIterator& { return it; })
        .def("__next__", &fdb5::ListIterator::next)
        .def("next", &fdb5::ListIterator::next);

    py::class_<eckit::DataHandle>(module, "DataHandle")
        .def("open", &eckit::DataHandle::openForRead)
        .def("close", &eckit::DataHandle::close)
        .def("skip", &eckit::DataHandle::skip)
        .def("seek", &eckit::DataHandle::seek)
        .def("tell", &eckit::DataHandle::position)
        .def("read", &eckit::DataHandle::read)
        .def("save_into", [](eckit::DataHandle& dh, const eckit::PathName& path){
                    dh.saveInto(path);
                });

    py::class_<fdb5::FDB>(module, "FDB")
        .def(py::init([](const fdb5::Config& config){
                    fdb_initialise();
                    return fdb5::FDB(config);
                    }))
        .def("list", &fdb5::FDB::list)
        .def("archive", pybind11::overload_cast<const void*, std::size_t>(&fdb5::FDB::archive), "Archive data given as a byte stream with the corresponding length")
        .def("archive", pybind11::overload_cast<const metkit::mars::MarsRequest&, eckit::DataHandle&>(&fdb5::FDB::archive), "Archive a MarsRequest and its corresponding DataHandle&")
        .def("flush", &fdb5::FDB::flush)
        .def("retrieve", &fdb5::FDB::retrieve)
        .doc() = "FDB instance.";
    
}
