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
/// @date   November 2018

#ifndef fdb5_api_StatusIterator_H
#define fdb5_api_StatusIterator_H

#include <string>

#include "eckit/filesystem/URI.h"

#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/Key.h"

namespace eckit {
    class Stream;
    class PathName;
}

namespace fdb5 {

class Catalogue;

//----------------------------------------------------------------------------------------------------------------------

/// Define a standard object which can be used to iterate the results of a
/// where() call on an arbitrary FDB object

struct StatusElement {

    StatusElement();
    StatusElement(const Catalogue& catalogue);
    StatusElement(eckit::Stream& s);

    // Database key
    Key key;

    // The location of the Database this response is for
    eckit::URI location;

    ControlIdentifiers controlIdentifiers;

private: // methods

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const StatusElement& e) {
        e.encode(s);
        return s;
    }
};

using StatusIterator = APIIterator<StatusElement>;

using StatusAggregateIterator = APIAggregateIterator<StatusElement>;

using StatusAsyncIterator = APIAsyncIterator<StatusElement>;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
