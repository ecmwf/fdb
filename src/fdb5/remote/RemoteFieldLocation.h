/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date Nov 2016

#ifndef fdb5_RemoteFieldLocation_H
#define fdb5_RemoteFieldLocation_H


#include "fdb5/database/FieldLocation.h"

namespace fdb5 {
namespace remote {


//----------------------------------------------------------------------------------------------------------------------

class RemoteFieldLocation : public FieldLocation {
public:

    RemoteFieldLocation(const FieldLocation& internal, const std::string& hostname, int port);
    RemoteFieldLocation(eckit::Stream&);
    RemoteFieldLocation(const RemoteFieldLocation&);

    virtual eckit::DataHandle *dataHandle(const Key& remapKey) const;
    virtual eckit::PathName url() const;
    virtual std::shared_ptr<FieldLocation> make_shared() const;
    virtual void visit(FieldLocationVisitor& visitor) const;

public: // For Streamable

    static const eckit::ClassSpec&  classSpec() { return classSpec_;}

protected: // For Streamable

    virtual void encode(eckit::Stream&) const;
    virtual const eckit::ReanimatorBase& reanimator() const { return reanimator_; }

    static eckit::ClassSpec                    classSpec_;
    static eckit::Reanimator<RemoteFieldLocation> reanimator_;

private: // methods

    virtual void dump(std::ostream &out) const;

    virtual void print(std::ostream &out) const;

private: // members

    std::string hostname_;
    int port_;

    std::shared_ptr<FieldLocation> internal_;
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5

#endif // fdb5_RemoteFieldLocation_H
