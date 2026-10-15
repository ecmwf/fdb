// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Mar 2016

#pragma once

#include <string>
#include <vector>

#include "metkit/mars/MarsRequest.h"


namespace eckit {
class Stream;
namespace option {
class CmdArgs;
}
}  // namespace eckit

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBToolRequest {
public:  // methods

    static std::vector<FDBToolRequest> requestsFromString(const std::string& request_str,
                                                          const std::vector<std::string> minimumKeys = {},
                                                          bool raw = false, const std::string& verb = "retrieve");

    FDBToolRequest(const metkit::mars::MarsRequest& r, bool all = false,
                   const std::vector<std::string>& minimumKeySet = std::vector<std::string>());

    FDBToolRequest(eckit::Stream&);

    const metkit::mars::MarsRequest& request() const;

    bool all() const;

    void print(std::ostream& s, const char* cr = "\n", const char* tab = "\t") const;

protected:  // methods

    void encode(eckit::Stream& s) const;

    friend std::ostream& operator<<(std::ostream& os, const FDBToolRequest& r) {
        os << "FDBToolRequest(";
        r.print(os, "", "");
        os << ")";
        return os;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const FDBToolRequest& r) {
        r.encode(s);
        return s;
    }

private:  // methods

    static void checkMinimumKeys(const metkit::mars::MarsRequest& request, const std::vector<std::string>& minimumKeys);

private:  // members

    metkit::mars::MarsRequest request_;

    bool all_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

