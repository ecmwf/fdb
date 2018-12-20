/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"

#include "fdb5/tools/ToolRequest.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ToolRequest::ToolRequest(const std::string& r, const std::vector<std::string>& minimumKeySet) :
    key_(r) {    

    for (std::vector<std::string>::const_iterator j = minimumKeySet.begin(); j != minimumKeySet.end(); ++j) {
        if (key_.find(*j) == key_.end()) {
            throw eckit::UserError("Please provide a value for '" + (*j) + "'");
        }
    }
}

const Key& ToolRequest::key() const
{
    return key_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

