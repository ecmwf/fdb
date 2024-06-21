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

    BaseArchiveVisitor(Archiver &owner, const CanonicalKey& initialFieldKey);

protected: // methods

    virtual bool selectDatabase(const CanonicalKey& dbKey, const TypedKey& fullComputedKey);

    virtual bool selectIndex(const CanonicalKey& idxKey, const TypedKey& fullComputedKey);

    virtual void checkMissingKeys(const TypedKey& fullComputedKey);

    virtual const Schema& databaseSchema() const;

    fdb5::DB* current() const;

private: // members

    Archiver &owner_;

    const CanonicalKey& initialFieldKey_;

    bool checkMissingKeysOnWrite_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
