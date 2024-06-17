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

/// @file   PoolGroup.h
/// @author Tiago Quintino
/// @date   Dec 2016

#ifndef fdb5_PoolGroup_H
#define fdb5_PoolGroup_H

#include <string>
#include <vector>

#include "eckit/utils/Regex.h"
#include "eckit/types/Types.h"

#include "fdb5/pmem/PoolEntry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class PoolGroup {

public: // methods

    PoolGroup(const std::string& name,
              const std::string& re,
              const std::string& handler,
              const std::vector<PoolEntry>& pools);

    /// Selects the pool where this CanonicalKey will be inserted
    /// @note This method must be idempotent -- it returns always the same value after the first call
    eckit::PathName pool(const CanonicalKey& key) const;

    void all(eckit::StringSet&) const;
    void writable(eckit::StringSet&) const;
    void visitable(eckit::StringSet&) const;

    bool match(const std::string& s) const;

    std::vector<eckit::PathName> writable() const;
    std::vector<eckit::PathName> visitable() const;

private: // members

    typedef std::vector<PoolEntry> PoolVec;

    std::string name_;

    std::string handler_;

    eckit::Regex re_;

    PoolVec pools_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
