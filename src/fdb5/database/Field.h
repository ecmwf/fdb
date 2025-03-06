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
/// @date Sep 2012

#ifndef fdb5_Field_H
#define fdb5_Field_H

#include "fdb5/database/FieldDetails.h"
#include "fdb5/database/FieldLocation.h"

#include "eckit/filesystem/PathName.h"

#include <ctime>
#include <memory>
#include <ostream>

namespace eckit {
class DataHandle;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class Field {

public:  // methods

    Field();

    Field(std::shared_ptr<const FieldLocation> location, time_t timestamp, FieldDetails details = FieldDetails());

    eckit::DataHandle* dataHandle() const { return location_->dataHandle(); }

    const FieldLocation& location() const { return *location_; }

    time_t timestamp() const { return timestamp_; }

    /// stableLocation is an object with validity that extends longer than that of the
    /// owning DB. May need converting to a more static form --- or not.
    std::shared_ptr<const FieldLocation> stableLocation() const { return location_->stableLocation(); }
    const FieldDetails& details() const { return details_; }

private:  // members

    std::shared_ptr<const FieldLocation> location_;

    time_t timestamp_{0};

    FieldDetails details_;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const Field& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
