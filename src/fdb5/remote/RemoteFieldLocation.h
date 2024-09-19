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

/// @author Simon Smart
/// @date Nov 2016

#ifndef fdb5_RemoteFieldLocation_H
#define fdb5_RemoteFieldLocation_H

#include "fdb5/database/FieldLocation.h"

namespace fdb5::remote {

class RemoteStore;

//----------------------------------------------------------------------------------------------------------------------

class RemoteFieldLocation : public FieldLocation {
public:

    RemoteFieldLocation(const eckit::net::Endpoint& endpoint, const FieldLocation& remoteLocation);
    RemoteFieldLocation(const eckit::net::Endpoint& endpoint, const RemoteFieldLocation& remoteLocation);
    RemoteFieldLocation(const eckit::URI& uri);
    RemoteFieldLocation(const eckit::URI& uri, const eckit::Offset& offset, const eckit::Length& length,
                        const Key& remapKey);
    RemoteFieldLocation(eckit::Stream&);
    RemoteFieldLocation(const RemoteFieldLocation&);

    eckit::DataHandle* dataHandle() const override;

    std::shared_ptr<const FieldLocation> make_shared() const override;
    void visit(FieldLocationVisitor& visitor) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    void encode(eckit::Stream&) const override;
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<RemoteFieldLocation> reanimator_;

private:  // methods

    void print(std::ostream& out) const override;

private:  // members
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote

#endif  // fdb5_RemoteFieldLocation_H
