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

#include "fdb5/remote/RemoteFieldLocationV1.h"

#include "eckit/exception/Exceptions.h"

namespace fdb5 {
namespace remote {

::eckit::ClassSpec RemoteFieldLocationV1::classSpec_ = {&FieldLocation::classSpec(), "RemoteFieldLocationV1",};
::eckit::Reanimator<RemoteFieldLocationV1> RemoteFieldLocationV1::reanimator_;

//----------------------------------------------------------------------------------------------------------------------


RemoteFieldLocationV1::RemoteFieldLocationV1(const FieldLocation& internal, const std::string& hostname, int port) :
    FieldLocation(eckit::URI("fdb", hostname, port)),
    hostname_(hostname),
    port_(port),
    internal_(internal.make_shared()) {}


RemoteFieldLocationV1::RemoteFieldLocationV1(eckit::Stream& s) :
    FieldLocation(s) {
    s >> hostname_;
    s >> port_;
    internal_.reset(eckit::Reanimator<FieldLocation>::reanimate(s));
}

RemoteFieldLocationV1::RemoteFieldLocationV1(const RemoteFieldLocationV1& rhs) :
    FieldLocation(eckit::URI("fdb", rhs.hostname_, rhs.port_)),
    hostname_(rhs.hostname_),
    port_(rhs.port_),
    internal_(rhs.internal_) {}


std::shared_ptr<FieldLocation> RemoteFieldLocationV1::make_shared() const {
    return std::make_shared<RemoteFieldLocationV1>(*this);
}

eckit::DataHandle* RemoteFieldLocationV1::dataHandle() const {
    return internal_->dataHandle();
}

eckit::DataHandle* RemoteFieldLocationV1::dataHandle(const Key& remapKey) const {
    return internal_->dataHandle(remapKey);
}

/*eckit::PathName RemoteFieldLocationV1::url() const {
    return internal_->url();
}*/

void RemoteFieldLocationV1::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

void RemoteFieldLocationV1::print(std::ostream& out) const {
    out << "[" << hostname_ << ":" << port_ << "]";
    out << *internal_;
}


void RemoteFieldLocationV1::encode(eckit::Stream& s) const {
    FieldLocation::encode(s);
    s << hostname_;
    s << port_;
    s << *internal_;
}

void RemoteFieldLocationV1::dump(std::ostream& out) const
{
    out << "  hostname: " << hostname_ << std::endl;
    out << "  port: " << hostname_ << std::endl;
    internal_->dump(out);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5
