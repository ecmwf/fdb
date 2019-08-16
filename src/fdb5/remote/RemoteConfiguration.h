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
/// @date   Mar 2018

#ifndef fdb5_remote_RemoteConfiguration_H
#define fdb5_remote_RemoteConfiguration_H

namespace eckit { class Stream; }

namespace fdb5 {

class FDB;

//----------------------------------------------------------------------------------------------------------------------

// This class handles negotiation between client/server for which functionality will be used
// over the wire
//
// n.b. This includes cases where there is version mismatches
//      --> The over-the-wire negotiation needs to take this into account.

#if 0

// TODO

class RemoteConfiguration {

public: // methods

    RemoteConfiguration();
    RemoteConfiguration(eckit::Stream& s);




private: // methods

    void encode(eckit::Stream& s) const;

private: // members

    friend eckit::Stream& operator<< (eckit::Stream& s, const RemoteConfiguration& rc) {
        rc.encode(s);
        return s;
    }


};

#endif


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_remote_RemoteFDB_H
