// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Jun 2016

#pragma once

#include <sys/types.h>

#include "eckit/types/FixedString.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct Indexer {

    Indexer();

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const Indexer& o) {
        o.print(s);
        return s;
    }

    pid_t pid_;
    pthread_t thread_;
    eckit::FixedString<64> hostname_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

