/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   BaseArchiveVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_BaseArchiveVisitor_H
#define fdb5_BaseArchiveVisitor_H

#include "fdb5/database/WriteVisitor.h"

namespace metkit { class MarsRequest; }

namespace fdb5 {

class Archiver;
class DB;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

class BaseArchiveVisitor : public WriteVisitor {

public: // methods

    BaseArchiveVisitor(Archiver& owner, const Key& initialFieldKey);

protected: // methods

    bool selectDatabase(const Key& dbKey, const TypedKey& fullComputedKey) override;

    bool selectIndex(const Key& idxKey, const TypedKey& fullComputedKey) override;

    virtual void checkMissingKeys(const TypedKey& fullComputedKey);

    const Schema& databaseSchema() const override;

    fdb5::DB* current() const;

    const Key& initialFieldKey() { return initialFieldKey_; } 

private: // members

    Archiver& owner_;

    const Key initialFieldKey_;
    bool checkMissingKeysOnWrite_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
