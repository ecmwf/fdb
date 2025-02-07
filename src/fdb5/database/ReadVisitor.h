/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ReadVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_ReadVisitor_H
#define fdb5_ReadVisitor_H

#include <iosfwd>

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"
#include "fdb5/database/Catalogue.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Key;
class TypesRegistry;
class Store;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

class ReadVisitor : public eckit::NonCopyable {

public:  // methods

    ReadVisitor() : catalogue_(nullptr) {}

    virtual ~ReadVisitor() {}

    virtual bool selectDatabase(const Key& dbKey, const Key& fullKey) = 0;
    virtual bool selectIndex(const Key& idxKey, const Key& fullKey)   = 0;
    virtual bool selectDatum(const Key& datumKey, const Key& fullKey) = 0;

    // Once we have selected a database, return its schema. Used for further iteration.
    virtual const Schema& databaseSchema() const = 0;

    virtual void values(const metkit::mars::MarsRequest& request, const std::string& keyword,
                        const TypesRegistry& registry, eckit::StringList& values) = 0;

protected:  // methods

    virtual void print(std::ostream& out) const = 0;

protected:  // members

    CatalogueReader* catalogue_;

private:  // members

    friend std::ostream& operator<<(std::ostream& s, const ReadVisitor& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
