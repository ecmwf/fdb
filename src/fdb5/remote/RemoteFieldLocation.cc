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
#include "fdb5/LibFdb5.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"
#include "fdb5/remote/client/RemoteStore.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URIManager.h"
#include "eckit/log/Log.h"

namespace fdb5 {
namespace remote {

::eckit::ClassSpec RemoteFieldLocation::classSpec_ = {
    &FieldLocation::classSpec(),
    "RemoteFieldLocation",
};
::eckit::Reanimator<RemoteFieldLocation> RemoteFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

RemoteFieldLocation::RemoteFieldLocation(const eckit::net::Endpoint& endpoint, const FieldLocation& remoteLocation) :
    FieldLocation(
        eckit::URI(RemoteFieldLocation::typeName(), remoteLocation.uri(), endpoint.hostname(), endpoint.port()),
        remoteLocation.offset(), remoteLocation.length(), remoteLocation.remapKey()) {

    ASSERT(remoteLocation.uri().scheme() != RemoteFieldLocation::typeName());
    if (!remoteLocation.uri().scheme().empty()) {
        uri_.query("internalScheme", remoteLocation.uri().scheme());
    }
    if (!remoteLocation.host().empty()) {
        uri_.query("internalHost", remoteLocation.host());
    }
}

RemoteFieldLocation::RemoteFieldLocation(const eckit::net::Endpoint& endpoint,
                                         const RemoteFieldLocation& remoteLocation) :
    FieldLocation(
        eckit::URI(RemoteFieldLocation::typeName(), remoteLocation.uri(), endpoint.hostname(), endpoint.port()),
        remoteLocation.offset(), remoteLocation.length(), remoteLocation.remapKey()) {}

RemoteFieldLocation::RemoteFieldLocation(const eckit::URI& uri) : FieldLocation(uri) {

    ASSERT(uri.scheme() == RemoteFieldLocation::typeName());
}

RemoteFieldLocation::RemoteFieldLocation(const eckit::URI& uri, const eckit::Offset& offset,
                                         const eckit::Length& length, const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {

    ASSERT(uri.scheme() == RemoteFieldLocation::typeName());
}

RemoteFieldLocation::RemoteFieldLocation(eckit::Stream& s) : FieldLocation(s) {}

RemoteFieldLocation::RemoteFieldLocation(const RemoteFieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}


std::shared_ptr<const FieldLocation> RemoteFieldLocation::make_shared() const {
    return std::make_shared<RemoteFieldLocation>(std::move(*this));
}

eckit::URI RemoteFieldLocation::internalURI(const eckit::URI& uri) {

    ASSERT(uri.scheme() == RemoteFieldLocation::typeName());
    // We need to remove the internalScheme and internalHost from the URI

    const std::string scheme   = uri.query("internalScheme");
    const std::string hostport = uri.query("internalHost");

    eckit::URI remote;
    if (hostport.empty()) {
        remote = eckit::URI(scheme, uri, "", -1);
    }
    else {
        eckit::net::Endpoint endpoint{hostport};
        remote = eckit::URI(scheme, uri, endpoint.host(), endpoint.port());
        remote.query("internalHost", "");
    }
    remote.query("internalScheme", "");
    return remote;
}

eckit::DataHandle* RemoteFieldLocation::dataHandle() const {

    if (fdb5::LibFdb5::instance().debug()) {
        eckit::Log::debug<fdb5::LibFdb5>() << "RemoteFieldLocation::dataHandle for location: ";
        dump(eckit::Log::debug<fdb5::LibFdb5>());
        eckit::Log::debug<fdb5::LibFdb5>() << std::endl;
    }

    RemoteStore& store = RemoteStore::get(uri_);

    eckit::URI remote = RemoteFieldLocation::internalURI(uri_);
    std::unique_ptr<FieldLocation> loc(
        FieldLocationFactory::instance().build(remote.scheme(), remote, offset_, length_, remapKey_));

    return store.dataHandle(*loc);
}

void RemoteFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

void RemoteFieldLocation::print(std::ostream& out) const {
    out << "RemoteFieldLocation[uri=" << uri_ << "]";
}

void RemoteFieldLocation::encode(eckit::Stream& s) const {
    FieldLocation::encode(s);
}

static FieldLocationBuilder<RemoteFieldLocation> builder(RemoteFieldLocation::typeName());

//----------------------------------------------------------------------------------------------------------------------

class FdbURIManager : public eckit::URIManager {
    bool authority() override { return true; }
    bool query() override { return true; }
    bool fragment() override { return false; }

    bool exists(const eckit::URI& f) override { return f.path().exists(); }

    eckit::DataHandle* newWriteHandle(const eckit::URI& f) override { return f.path().fileHandle(); }

    eckit::DataHandle* newReadHandle(const eckit::URI& f) override { return f.path().fileHandle(); }

    eckit::DataHandle* newReadHandle(const eckit::URI& f, const eckit::OffsetList& ol,
                                     const eckit::LengthList& ll) override {
        return f.path().partHandle(ol, ll);
    }

    eckit::PathName path(const eckit::URI& u) const override { return eckit::PathName{u.name()}; }

    std::string asString(const eckit::URI& uri) const override {
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

}  // namespace remote
}  // namespace fdb5