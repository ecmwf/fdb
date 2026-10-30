// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#pragma once

#include "eckit/eckit.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/types/FixedString.h"
#include "eckit/types/Types.h"

#include "fdb5/database/FieldDetails.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"

namespace eckit {
class DataHandle;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class Field {

public:  // methods

    Field();

    Field(std::shared_ptr<const FieldLocation> location, time_t timestamp,
          const FieldDetails& details = FieldDetails());
    Field(const FieldLocation&& location, time_t timestamp, const FieldDetails& details = FieldDetails());

    eckit::DataHandle* dataHandle() const { return location_->dataHandle(); }

    const FieldLocation& location() const { return *location_; }

    time_t timestamp() const { return timestamp_; }

    /// stableLocation is an object with validity that extends longer than that of the
    /// owning DB. May need converting to a more static form --- or not.
    std::shared_ptr<const FieldLocation> stableLocation() const { return location_->stableLocation(); }
    const FieldDetails& details() const { return details_; }

private:  // members

    std::shared_ptr<const FieldLocation> location_;

    time_t timestamp_;

    FieldDetails details_;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const Field& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
