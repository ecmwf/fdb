// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date Nov 2016

#pragma once

#include "fdb5/database/FieldLocation.h"

namespace fdb5::remote {

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

    static const char* typeName() { return "fdb"; }

    eckit::DataHandle* dataHandle() const override;

    std::shared_ptr<const FieldLocation> make_shared() const override;
    void visit(FieldLocationVisitor& visitor) const override;
    static eckit::URI internalURI(const eckit::URI& uri);

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
