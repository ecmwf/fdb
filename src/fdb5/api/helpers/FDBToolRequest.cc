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

#include "fdb5/api/helpers/FDBToolRequest.h"

#include <algorithm>
#include <iterator>

#include "metkit/mars/MarsExpension.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsRequest.h"

#include "eckit/exception/Exceptions.h"

#include "fdb5/LibFdb5.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


std::vector<FDBToolRequest> FDBToolRequest::requestsFromString(const std::string& request_str,
                                                               const std::vector<std::string> minimumKeys,
                                                               bool raw,
                                                               const std::string& verb) {


    const std::string full_string = verb + "," + request_str; // Use a dummy verb
    std::istringstream istringstream(full_string);
    metkit::mars::MarsParser parser(istringstream);
    auto parsedRequests = parser.parse();
    ASSERT(parsedRequests.size() == 1);

    for (const auto& request : parsedRequests) {
        LOG_DEBUG_LIB(LibFdb5) << "Parsed request: " << static_cast<const metkit::mars::MarsRequest&>(request) << std::endl;
        checkMinimumKeys(request, minimumKeys);
    }

    std::vector<FDBToolRequest> requests;

    if (raw) {
        requests = std::vector<FDBToolRequest>(parsedRequests.begin(), parsedRequests.end());
    } else {

        /*// We want to use (default) inherited requests, as this allows use to use
        // TypeParam with a certain amount of meaning. But we also want to be able
        // to use sparse requests for the FDB. So we unset anything that has not
        // been originally specified by the user.

        auto&& ps(parsedRequests.front().params());
        std::set<std::string> setParams(std::make_move_iterator(ps.begin()),
                                        std::make_move_iterator(ps.end()));*/

        const bool inherit = false;
        const bool strict = true;
        metkit::mars::MarsExpension expand(inherit, strict);
        auto expandedRequests = expand.expand(parsedRequests);

        for (metkit::mars::MarsRequest& request : expandedRequests) {

            /*for (const auto& param : request.params()) {
                if (std::find(setParams.begin(), setParams.end(), param) == setParams.end()) {
                    request.unsetValues(param);
                }
            }*/
            LOG_DEBUG_LIB(LibFdb5) << "Expanded request: " << request << std::endl;
            requests.emplace_back(request, false, minimumKeys);
        }
    }

    return requests;
}


FDBToolRequest::FDBToolRequest(const metkit::mars::MarsRequest& marsrequest,
                               bool all,
                               const std::vector<std::string>& minimumKeySet) :
    request_(marsrequest),
    all_(all) {

    checkMinimumKeys(request_, minimumKeySet);
}

FDBToolRequest::FDBToolRequest(eckit::Stream& stream) :
    request_(stream) {
    stream >> all_;
}

const metkit::mars::MarsRequest& FDBToolRequest::request() const {
    return request_;
}

bool FDBToolRequest::all() const {
    return all_;

}

void FDBToolRequest::print(std::ostream &ostream, const char* carrierReturn, const char* tab) const {

    if (all_) {
        ostream << " -- ALL --";
    } else {
        request_.dump(ostream, carrierReturn, tab);
    }
}

void FDBToolRequest::encode(eckit::Stream& stream) const {
    stream << request_;
    stream << all_;
}

void FDBToolRequest::checkMinimumKeys(const metkit::mars::MarsRequest& request, const std::vector<std::string>& minimumKeys) {
    for (const auto & minimumKey : minimumKeys) {
        if (!request.has(minimumKey)) {
            throw eckit::UserError("Please provide a value for '" + minimumKey + "'");
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

