// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   January 2024

#pragma once

#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"

namespace eckit {
class Stream;
class JSON;
}  // namespace eckit

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class AxesElement {
public:  // methods

    AxesElement() = default;
    AxesElement(Key&& dbKey, IndexAxis&& axis);
    explicit AxesElement(eckit::Stream& s);

    [[nodiscard]]
    const Key& key() const {
        return dbKey_;
    }

    [[nodiscard]]
    const IndexAxis& axes() const {
        return axes_;
    }

    void print(std::ostream& out) const;
    size_t encodeSize() const;

private:  // methods

    void encode(eckit::Stream& s) const;

    friend std::ostream& operator<<(std::ostream& os, const AxesElement& e) {
        e.print(os);
        return os;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const AxesElement& r) {
        r.encode(s);
        return s;
    }

private:  // members

    Key dbKey_;
    IndexAxis axes_;
};

//----------------------------------------------------------------------------------------------------------------------

using AxesAggregateIterator = APIAggregateIterator<AxesElement>;

using AxesIterator = APIIterator<AxesElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
