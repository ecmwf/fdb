/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/FDBAggregateListObjects.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDBAggregateListObjects::FDBAggregateListObjects(std::queue<fdb5::FDBListObject>&& listObjects)
    : listObjects_(std::move(listObjects)) {}

FDBAggregateListObjects::~FDBAggregateListObjects() {}

bool FDBAggregateListObjects::next(FDBListElement& elem) {

    while (!listObjects_.empty()) {

        if (listObjects_.front().next(elem)) {
            return true;
        }

        listObjects_.pop();
    }

    return false;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

