// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   ReadVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include <iosfwd>

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

class ReadVisitor {

public:  // methods

    ReadVisitor() {}

    ReadVisitor(const ReadVisitor&) = delete;
    ReadVisitor& operator=(const ReadVisitor&) = delete;
    ReadVisitor(ReadVisitor&&) = delete;
    ReadVisitor& operator=(ReadVisitor&&) = delete;

    virtual ~ReadVisitor() {}

    virtual bool selectDatabase(const Key& dbKey, const Key& fullKey) = 0;
    virtual bool selectIndex(const Key& idxKey) = 0;
    virtual bool selectDatum(const Key& datumKey, const Key& fullKey) = 0;

    virtual void deselectDatabase() = 0;

    // Once we have selected a database, return its schema. Used for further iteration.
    virtual const Schema& databaseSchema() const = 0;

    void values(const metkit::mars::MarsRequest& request, const std::string& keyword, const TypesRegistry& registry,
                eckit::StringList& values);

protected:  // methods

    virtual void print(std::ostream& out) const = 0;

protected:  // members

    CatalogueReader* catalogue_{};

    friend std::ostream& operator<<(std::ostream& s, const ReadVisitor& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
