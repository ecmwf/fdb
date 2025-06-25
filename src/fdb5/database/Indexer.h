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
/// @date   Jun 2016

#ifndef fdb5_Indexer_H
#define fdb5_Indexer_H

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

#endif
