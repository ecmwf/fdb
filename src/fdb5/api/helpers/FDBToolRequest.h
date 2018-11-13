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

#include "fdb5/database/Key.h"


namespace eckit { namespace option { class CmdArgs; }}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBToolRequest {
public: // methods

    FDBToolRequest(const std::string& r,
                   bool all=false,
                   const std::vector<std::string>& minimumKeySet = std::vector<std::string>());

    const Key& key() const;

    bool all() const;

protected: // methods

    void print(std::ostream& s) const;

    friend std::ostream& operator<<(std::ostream& os, const FDBToolRequest& r) {
        r.print(os);
        return os;
    }

private: // methods

    Key key_;

    bool all_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
