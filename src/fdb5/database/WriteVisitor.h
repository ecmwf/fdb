/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   WriteVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_WriteVisitor_H
#define fdb5_WriteVisitor_H

#include <iosfwd>
#include <vector>

#include "eckit/memory/NonCopyable.h"

#include "fdb5/database/Key.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Rule;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

class WriteVisitor : public eckit::NonCopyable {

public:  // methods

    WriteVisitor(std::vector<Key>&);

    virtual ~WriteVisitor() = default;

    virtual bool selectDatabase(const Key& dbKey, const Key& fullKey) = 0;
    virtual bool selectIndex(const Key& idxKey, const Key& fullKey)   = 0;
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

#endif
