/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/RemoteFieldLocation.h"

#include "eckit/exception/Exceptions.h"

namespace fdb5 {
namespace remote {

::eckit::ClassSpec RemoteFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "RemoteFieldLocation",};
::eckit::Reanimator<RemoteFieldLocation> RemoteFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------


RemoteFieldLocation::RemoteFieldLocation(const FieldLocation& internal, const std::string& hostname, int port) :
    FieldLocation(internal.length()),
    hostname_(hostname),
    port_(port),
    internal_(internal.make_shared()) {}


RemoteFieldLocation::RemoteFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {
    s >> hostname_;
    s >> port_;
    internal_.reset(eckit::Reanimator<FieldLocation>::reanimate(s));
}

RemoteFieldLocation::RemoteFieldLocation(const RemoteFieldLocation& rhs) :
    FieldLocation(rhs.length()),
    hostname_(rhs.hostname_),
    port_(rhs.port_),
    internal_(rhs.internal_) {}


std::shared_ptr<FieldLocation> RemoteFieldLocation::make_shared() const {
    return std::make_shared<RemoteFieldLocation>(*this);
}

eckit::DataHandle* RemoteFieldLocation::dataHandle(const Key& remapKey) const {
    return internal_->dataHandle(remapKey);
}

eckit::PathName RemoteFieldLocation::url() const {
    return internal_->url();
}

void RemoteFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

void RemoteFieldLocation::print(std::ostream& out) const {
    out << "[" << hostname_ << ":" << port_ << "]";
    out << *internal_;
}


void RemoteFieldLocation::encode(eckit::Stream& s) const {
    FieldLocation::encode(s);
    s << hostname_;
    s << port_;
    s << *internal_;
}

void RemoteFieldLocation::dump(std::ostream& out) const
{
    out << "  hostname: " << hostname_ << std::endl;
    out << "  port: " << hostname_ << std::endl;
    internal_->dump(out);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5
