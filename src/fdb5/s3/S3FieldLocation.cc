/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "eckit/filesystem/URIManager.h"
#include "fdb5/daos/S3FieldLocation.h"
// #include "fdb5/LibFdb5.h"

namespace fdb5 {

::eckit::ClassSpec S3FieldLocation::classSpec_ = {&FieldLocation::classSpec(), "S3FieldLocation",};
::eckit::Reanimator<S3FieldLocation> S3FieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

S3FieldLocation::S3FieldLocation(const S3FieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

S3FieldLocation::S3FieldLocation(const eckit::URI &uri) : FieldLocation(uri) {}

/// @todo: remove remapKey from signature and always pass empty Key to FieldLocation
S3FieldLocation::S3FieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length, const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

S3FieldLocation::S3FieldLocation(eckit::Stream& s) :
    FieldLocation(s) {}

std::shared_ptr<FieldLocation> S3FieldLocation::make_shared() const {
    return std::make_shared<S3FieldLocation>(std::move(*this));
}

eckit::DataHandle* S3FieldLocation::dataHandle() const {

    return eckit::S3Name(uri_).dataHandle(offset(), length());
    
}

void S3FieldLocation::print(std::ostream &out) const {
    out << "S3FieldLocation[uri=" << uri_ << "]";
}

void S3FieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

static FieldLocationBuilder<S3FieldLocation> builder("s3");

//----------------------------------------------------------------------------------------------------------------------

// class DaosURIManager : public eckit::URIManager {
//     virtual bool query() override { return true; }
//     virtual bool fragment() override { return true; }

//     virtual eckit::PathName path(const eckit::URI& f) const override { return f.name(); }

//     virtual bool exists(const eckit::URI& f) override {

//         return fdb5::DaosName(f).exists();

//     }

//     virtual eckit::DataHandle* newWriteHandle(const eckit::URI& f) override {

//         if (fdb5::DaosName(f).OID().otype() != DAOS_OT_ARRAY) NOTIMP;
        
//         return fdb5::DaosArrayName(f).dataHandle();
        
//     }

//     virtual eckit::DataHandle* newReadHandle(const eckit::URI& f) override {

//         if (fdb5::DaosName(f).OID().otype() != DAOS_OT_ARRAY) NOTIMP;
        
//         return fdb5::DaosArrayName(f).dataHandle();
        
//     }

//     virtual eckit::DataHandle* newReadHandle(const eckit::URI& f, const eckit::OffsetList& ol, const eckit::LengthList& ll) override {

//         if (fdb5::DaosName(f).OID().otype() != DAOS_OT_ARRAY) NOTIMP;
        
//         return fdb5::DaosArrayName(f).dataHandle();
        
//     }

//     virtual std::string asString(const eckit::URI& uri) const override {
//         std::string q = uri.query();
//         if (!q.empty())
//             q = "?" + q;
//         std::string f = uri.fragment();
//         if (!f.empty())
//             f = "#" + f;

//         return uri.scheme() + ":" + uri.name() + q + f;
//     }
// public:
//     DaosURIManager(const std::string& name) : eckit::URIManager(name) {}
// };

// static DaosURIManager daos_uri_manager("daos");

} // namespace fdb5