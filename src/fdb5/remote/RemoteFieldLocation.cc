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

#include "eckit/exception/Exceptions.h"

namespace fdb5 {
namespace remote {

::eckit::ClassSpec RemoteFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "RemoteFieldLocation",};
::eckit::Reanimator<RemoteFieldLocation> RemoteFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------


RemoteFieldLocation::RemoteFieldLocation(const FieldLocation& internal, const std::string& hostname, int port) :
    FieldLocation(eckit::URI("fdb", internal.uri(), hostname, port)) {}

RemoteFieldLocation::RemoteFieldLocation(const eckit::URI &uri) : FieldLocation(uri) {
    // internal_.reset(FieldLocationFactory::instance().build(uri.scheme(), uri));
    // should we forbid this constructor ?
}

RemoteFieldLocation::RemoteFieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length) : FieldLocation(uri, offset, length) {
    // internal_.reset(FieldLocationFactory::instance().build(uri.scheme(), uri, offset, length));
    // should we forbid this constructor ?
}

RemoteFieldLocation::RemoteFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {
    internal_.reset(eckit::Reanimator<FieldLocation>::reanimate(s));
}

RemoteFieldLocation::RemoteFieldLocation(const RemoteFieldLocation& rhs) :
    FieldLocation(rhs.uri_),
    internal_(rhs.internal_) {}


std::shared_ptr<FieldLocation> RemoteFieldLocation::make_shared() const {
    return std::make_shared<RemoteFieldLocation>(*this);
}

eckit::DataHandle* RemoteFieldLocation::dataHandle() const {
    return internal_->dataHandle();
}

eckit::DataHandle* RemoteFieldLocation::dataHandle(const Key& remapKey) const {
    return internal_->dataHandle(remapKey);
}

/*eckit::PathName RemoteFieldLocation::url() const {
    return internal_->url();
}*/

void RemoteFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

void RemoteFieldLocation::print(std::ostream& out) const {
//    out << "[" << hostname_ << ":" << port_ << "]";
    out << *internal_;
}


void RemoteFieldLocation::encode(eckit::Stream& s) const {
    FieldLocation::encode(s);
    s << *internal_;
}

void RemoteFieldLocation::dump(std::ostream& out) const
{
//    out << "  hostname: " << hostname_ << std::endl;
//    out << "  port: " << hostname_ << std::endl;
    internal_->dump(out);
}

static FieldLocationBuilder<RemoteFieldLocation> builder("fdb");

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5
