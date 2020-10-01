/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/api/RemoteFDB.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URIManager.h"

namespace fdb5 {
namespace remote {

::eckit::ClassSpec RemoteFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "RemoteFieldLocation",};
::eckit::Reanimator<RemoteFieldLocation> RemoteFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

RemoteFieldLocation::RemoteFieldLocation(RemoteFDB* remoteFDB, const FieldLocation& remoteLocation) :
    FieldLocation(eckit::URI("fdb", remoteFDB->controlEndpoint().host(),  remoteFDB->controlEndpoint().port())),
    remoteFDB_(remoteFDB),
    internal_(remoteLocation.make_shared()) {
    ASSERT(remoteFDB);
    ASSERT(remoteLocation.uri().scheme() != "fdb");
}

RemoteFieldLocation::RemoteFieldLocation(const eckit::URI& uri, const eckit::Offset& offset, const eckit::Length& length, const Key& remapKey) :
    FieldLocation(eckit::URI("fdb", uri), offset, length, remapKey) {
    NOTIMP;
}

RemoteFieldLocation::RemoteFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {
    //internal_.reset(eckit::Reanimator<FieldLocation>::reanimate(s));
    NOTIMP;
}

RemoteFieldLocation::RemoteFieldLocation(const RemoteFieldLocation& rhs) :
    FieldLocation(rhs.uri_),
    remoteFDB_(rhs.remoteFDB_),
    internal_(rhs.internal_) {}


std::shared_ptr<FieldLocation> RemoteFieldLocation::make_shared() const {
    return std::make_shared<RemoteFieldLocation>(*this);
}

eckit::DataHandle* RemoteFieldLocation::dataHandle() const {
    ASSERT(remoteFDB_);
    ASSERT(internal_);
    return remoteFDB_->dataHandle(*internal_);
}

void RemoteFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

void RemoteFieldLocation::print(std::ostream& out) const {
    out << "RemoteFieldLocation[uri=" << uri_ << ",internal=" << *internal_ << "]";
}

void RemoteFieldLocation::encode(eckit::Stream& s) const {
    FieldLocation::encode(s);
    s << *internal_;
    std::stringstream ss;
    ss << remoteFDB_->config();
    s << ss.str();
}

void RemoteFieldLocation::dump(std::ostream& out) const {
    out << "  uri: " << uri_ << std::endl;
    internal_->dump(out);
}

static FieldLocationBuilder<RemoteFieldLocation> builder("fdb");

//----------------------------------------------------------------------------------------------------------------------

class FdbURIManager : public eckit::URIManager {
    virtual bool query() override { return false; }
    virtual bool fragment() override { return false; }

    virtual bool exists(const eckit::URI& f) override { return f.path().exists(); }

    virtual eckit::DataHandle* newWriteHandle(const eckit::URI& f) override { return f.path().fileHandle(); }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f) override { return f.path().fileHandle(); }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f, const eckit::OffsetList& ol, const eckit::LengthList& ll) override {
        return f.path().partHandle(ol, ll);
    }

    virtual std::string asString(const eckit::URI& uri) const override {
        std::string q = uri.query();
        if (!q.empty())
            q = "?" + q;
        std::string f = uri.fragment();
        if (!f.empty())
            f = "#" + f;

        return uri.name() + q + f;
    }
public:
    FdbURIManager(const std::string& name) : eckit::URIManager(name) {}
};

static FdbURIManager manager_fdb_file("fdb");

} // namespace remote
} // namespace fdb5
