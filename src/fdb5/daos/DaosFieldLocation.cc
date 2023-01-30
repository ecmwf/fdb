/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/filesystem/URIManager.h"
// #include "eckit/io/rados/RadosReadHandle.h"
#include "fdb5/daos/DaosFieldLocation.h"
#include "fdb5/daos/DaosName.h"
// #include "fdb5/LibFdb5.h"
// #include "fdb5/io/SingleGribMungePartFileHandle.h"

namespace fdb5 {

// ::eckit::ClassSpec RadosFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "RadosFieldLocation",};
// ::eckit::Reanimator<RadosFieldLocation> RadosFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

// //RadosFieldLocation::RadosFieldLocation() {}

DaosFieldLocation::DaosFieldLocation(const DaosFieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

// DaosFieldLocation::DaosFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length) :
//     FieldLocation(eckit::URI("daos", path), offset, length) {}

// DaosFieldLocation::DaosFieldLocation(const eckit::URI &uri) :
//     FieldLocation(uri) {}

DaosFieldLocation::DaosFieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length, const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

// DaosFieldLocation::DaosFieldLocation(const FileStore &store, const FieldRef &ref) :
//     FieldLocation(store.get(ref.pathId()), ref.offset(), ref.length()) {}

// DaosFieldLocation::DaosFieldLocation(eckit::Stream& s) :
//     FieldLocation(s) {}

std::shared_ptr<FieldLocation> DaosFieldLocation::make_shared() const {
    return std::make_shared<DaosFieldLocation>(*this);
}

eckit::DataHandle* DaosFieldLocation::dataHandle() const {

    // TODO: ensure DaosSession has been configured before any actions on DaosNames in DaosFieldLocation
    return fdb5::DaosName(uri_).dataHandle();
    
}

// eckit::DataHandle *DaosFieldLocation::dataHandle(const Key& remapKey) const {
//     return new SingleGribMungePartFileHandle(path(), offset(), length(), remapKey);
// }

void DaosFieldLocation::print(std::ostream &out) const {
    out << "DaosFieldLocation[uri=" << uri_ << "]";
}

void DaosFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

// eckit::URI DaosFieldLocation::uri(const eckit::PathName &path) {
//     return eckit::URI("daos", path);
// }

static FieldLocationBuilder<DaosFieldLocation> builder("daos");

//----------------------------------------------------------------------------------------------------------------------

class DaosURIManager : public eckit::URIManager {
    virtual bool query() override { return true; }
    virtual bool fragment() override { return false; }

    virtual eckit::PathName path(const eckit::URI& f) const override { return f.name(); }

    virtual bool exists(const eckit::URI& f) override { return fdb5::DaosName(f).exists(); }

    virtual eckit::DataHandle* newWriteHandle(const eckit::URI& f) override { return fdb5::DaosName(f).dataHandle(); }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f) override { return fdb5::DaosName(f).dataHandle(); }

    // TODO: implement DaosName::partHandle
    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f, const eckit::OffsetList& ol, const eckit::LengthList& ll) override {
        return fdb5::DaosName(f).dataHandle();
    }

    virtual std::string asString(const eckit::URI& uri) const override {
        std::string q = uri.query();
        if (!q.empty())
            q = "?" + q;
        std::string f = uri.fragment();
        if (!f.empty())
            f = "#" + f;

        return uri.scheme() + "://" + uri.name() + q + f;
    }
public:
    DaosURIManager(const std::string& name) : eckit::URIManager(name) {}
};

static DaosURIManager daos_uri_manager("daos");

} // namespace fdb5
