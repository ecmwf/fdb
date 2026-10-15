// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   BaseArchiveVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include "fdb5/database/WriteVisitor.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Archiver;
class CatalogueWriter;
class Store;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

class BaseArchiveVisitor : public WriteVisitor {

public:  // methods

    BaseArchiveVisitor(Archiver& owner, const Key& initialFieldKey);

protected:  // methods

    bool selectDatabase(const Key& dbKey, const Key&) override;

    bool selectIndex(const Key& idxKey) override;
    bool createIndex(const Key& idxKey, size_t datumKeySize) override;

    virtual void checkMissingKeys(const Key& fullKey) const;

    const Schema& databaseSchema() const override;

    std::shared_ptr<CatalogueWriter> catalogue() const;
    Store* store() const;

    const Key& initialFieldKey() const { return initialFieldKey_; }

private:  // members

    Archiver& owner_;

    Key initialFieldKey_;

    bool checkMissingKeysOnWrite_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
