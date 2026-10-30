// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   WriteVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include <iosfwd>
#include <vector>

#include "fdb5/database/Key.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Rule;
class RuleDatum;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

class WriteVisitor {

public:  // methods

    WriteVisitor(std::vector<Key>&);

    WriteVisitor(const WriteVisitor&) = delete;
    WriteVisitor& operator=(const WriteVisitor&) = delete;
    WriteVisitor(WriteVisitor&&) = delete;
    WriteVisitor& operator=(WriteVisitor&&) = delete;

    virtual ~WriteVisitor() = default;

    virtual bool selectDatabase(const Key& dbKey, const Key& fullKey) = 0;
    virtual bool selectIndex(const Key& idxKey) = 0;
    virtual bool createIndex(const Key& idxKey, size_t datumKeySize) = 0;
    virtual bool selectDatum(const Key& datumKey, const Key& fullKey) = 0;

    // Once we have selected a database, return its schema. Used for further iteration.
    virtual const Schema& databaseSchema() const = 0;

    void rule(const Rule* r) { rule_ = r; }
    const Rule* rule() const { return rule_; }

protected:  // methods

    virtual void print(std::ostream& out) const = 0;

private:  // members

    friend std::ostream& operator<<(std::ostream& s, const WriteVisitor& x) {
        x.print(s);
        return s;
    }

    friend class Rule;

    std::vector<Key>& prev_;

    const Rule* rule_{nullptr};  // Last rule used
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
