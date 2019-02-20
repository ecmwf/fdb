/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Mar 2016

#ifndef fdb5_FDBToolRequest_H
#define fdb5_FDBToolRequest_H

#include <string>
#include <vector>

#include "metkit/MarsRequest.h"


namespace eckit {
    class Stream;
    namespace option { class CmdArgs; }
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBToolRequest {
public: // methods

    static std::vector<FDBToolRequest> requestsFromString(const std::string& request_str,
                                                          const std::vector<std::string> minimumKeys = {});

    FDBToolRequest(const metkit::MarsRequest& r,
                   bool all=false,
                   const std::vector<std::string>& minimumKeySet = std::vector<std::string>());

    FDBToolRequest(eckit::Stream&);

    const metkit::MarsRequest& request() const;

    bool all() const;

protected: // methods

    void print(std::ostream& s) const;
    void encode(eckit::Stream& s) const;

    friend std::ostream& operator<<(std::ostream& os, const FDBToolRequest& r) {
        r.print(os);
        return os;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const FDBToolRequest& r) {
        r.encode(s);
        return s;
    }

private: // methods

    metkit::MarsRequest request_;

    bool all_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
