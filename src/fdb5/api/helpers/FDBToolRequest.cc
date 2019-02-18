/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/FDBToolRequest.h"

#include "eckit/exception/Exceptions.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


FDBToolRequest::FDBToolRequest(const std::string& r,
                               bool all,
                               const std::vector<std::string>& minimumKeySet) :
    key_(r),
    all_(all) {

    for (std::vector<std::string>::const_iterator j = minimumKeySet.begin(); j != minimumKeySet.end(); ++j) {
        if (key_.find(*j) == key_.end()) {
            throw eckit::UserError("Please provide a value for '" + (*j) + "'");
        }
    }
}

FDBToolRequest::FDBToolRequest(eckit::Stream& s) :
    key_(s) {
    s >> all_;
}

const Key& FDBToolRequest::key() const {
    return key_;
}

bool FDBToolRequest::all() const {
    return all_;

}

void FDBToolRequest::print(std::ostream &s) const {
    s << "FDBToolRequest(" << key_;
    s << " : all=" << (all_ ? "true" : "false");
    s << ")";

}

void FDBToolRequest::encode(eckit::Stream& s) const {
    s << key_;
    s << all_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

