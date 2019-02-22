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

#include "metkit/MarsExpension.h"
#include "metkit/MarsParser.h"
#include "metkit/MarsRequest.h"

#include "eckit/exception/Exceptions.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


std::vector<FDBToolRequest> FDBToolRequest::requestsFromString(const std::string& request_str,
                                                               const std::vector<std::string> minimumKeys) {


    std::string full_string = "retrieve," + request_str; // Use a dummy verb
    std::istringstream in(full_string);
    metkit::MarsParser parser(in);
    auto parsedRequests = parser.parse();

    // we DON'T fully expand this request. We don't want to force full-qualification,
    // or end up with defaults like date, ...
    // Also, we want to be able to test that keys are missing internally.

    std::vector<FDBToolRequest> requests;

    bool inherit = false;
    metkit::MarsExpension expand(inherit);
    auto expandedRequests = expand.expand(parsedRequests);

    for (const auto& request : expandedRequests) {
        requests.emplace_back(FDBToolRequest(request, false, minimumKeys));
    }

    return requests;
}


FDBToolRequest::FDBToolRequest(const metkit::MarsRequest& r,
                               bool all,
                               const std::vector<std::string>& minimumKeySet) :
    request_(r),
    all_(all) {

    for (std::vector<std::string>::const_iterator j = minimumKeySet.begin(); j != minimumKeySet.end(); ++j) {
        if (!request_.has(*j)) {
            throw eckit::UserError("Please provide a value for '" + (*j) + "'");
        }
    }
}

FDBToolRequest::FDBToolRequest(eckit::Stream& s) :
    request_(s) {
    s >> all_;
}

const metkit::MarsRequest& FDBToolRequest::request() const {
    return request_;
}

bool FDBToolRequest::all() const {
    return all_;

}

void FDBToolRequest::print(std::ostream &s) const {
    s << "FDBToolRequest(" << request_;
    s << " : all=" << (all_ ? "true" : "false");
    s << ")";

}

void FDBToolRequest::encode(eckit::Stream& s) const {
    s << request_;
    s << all_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

