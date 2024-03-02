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
#include "eckit/io/rados/RadosObject.h"
// #include "eckit/io/rados/RadosMultiObjReadHandle.h"

#include "fdb5/rados/RadosFieldLocation.h"
// #include "fdb5/LibFdb5.h"
// #include "fdb5/io/SingleGribMungePartFileHandle.h"

namespace fdb5 {

::eckit::ClassSpec RadosFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "RadosFieldLocation",};
::eckit::Reanimator<RadosFieldLocation> RadosFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

static FieldLocationBuilder<RadosFieldLocation> builder("rados");

// RadosFieldLocation::RadosFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length ) :
//         FieldLocation(eckit::URI("rados", path), offset, length) {}

RadosFieldLocation::RadosFieldLocation(const RadosFieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

RadosFieldLocation::RadosFieldLocation(const eckit::URI &uri) : FieldLocation(uri) {}

/// @todo: remove remapKey from signature and always pass empty Key to FieldLocation
RadosFieldLocation::RadosFieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length, const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

// RadosFieldLocation::RadosFieldLocation(const FileStore &store, const FieldRef &ref) :
//     FieldLocation(store.get(ref.pathId()), ref.offset(), ref.length()) {}

RadosFieldLocation::RadosFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {}

std::shared_ptr<FieldLocation> RadosFieldLocation::make_shared() const {
    return std::make_shared<RadosFieldLocation>(std::move(*this));
}

eckit::DataHandle* RadosFieldLocation::dataHandle() const {

#if defined(fdb5_HAVE_RADOS_STORE_MULTIPART) && ! defined(fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD)

    return eckit::RadosObject(uri_).multipartRangeReadHandle(offset(), length());

#else

    return eckit::RadosObject(uri_).rangeReadHandle(offset(), length());

#endif

}

// eckit::DataHandle *RadosFieldLocation::dataHandle(const Key& remapKey) const {
//     return new SingleGribMungePartFileHandle(path(), offset(), length(), remapKey);
// }

void RadosFieldLocation::print(std::ostream &out) const {
    out << "RadosFieldLocation[uri=" << uri_ << "]";
}

void RadosFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

// eckit::URI RadosFieldLocation::uri(const eckit::PathName &path) {
//     return eckit::URI("rados", path);
// }

//----------------------------------------------------------------------------------------------------------------------

class RadosURIManager : public eckit::URIManager {
    virtual bool query() override { return true; }
    virtual bool fragment() override { return true; }

    // virtual eckit::PathName path(const eckit::URI& f) const override { return f.name(); }

    virtual bool exists(const eckit::URI& f) override {

        return eckit::RadosObject(f).exists();

    }

    virtual eckit::DataHandle* newWriteHandle(const eckit::URI& f) override {
        
        return eckit::RadosObject(f).dataHandle();
        
    }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f) override {
        
        return eckit::RadosObject(f).dataHandle();
        
    }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f, const eckit::OffsetList& ol, const eckit::LengthList& ll) override {

        NOTIMP;
        
    }

    virtual std::string asString(const eckit::URI& uri) const override {
        std::string q = uri.query();
        if (!q.empty())
            q = "?" + q;
        std::string f = uri.fragment();
        if (!f.empty())
            f = "#" + f;

        return uri.scheme() + ":" + uri.name() + q + f;
    }
public:
    RadosURIManager(const std::string& name) : eckit::URIManager(name) {}
};

static RadosURIManager rados_uri_manager("rados");

} // namespace fdb5