/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   MultiRetrieveVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   October 2016

#ifndef fdb5_MultiRetrieveVisitor_H
#define fdb5_MultiRetrieveVisitor_H

#include <string>

#include "eckit/container/CacheLRU.h"
#include "eckit/container/Queue.h"

#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Inspector.h"
#include "fdb5/database/ReadVisitor.h"

namespace fdb5 {

class HandleGatherer;
class Notifier;

class DB;

//----------------------------------------------------------------------------------------------------------------------

class MultiRetrieveVisitor : public ReadVisitor {

public: // methods

    MultiRetrieveVisitor(const Notifier& wind,
                         InspectIterator& queue,
                         eckit::CacheLRU<Key,DB*>& databases,
                         const Config& config);

    ~MultiRetrieveVisitor();

private:  // methods

    // From Visitor

    bool selectDatabase(const Key& dbKey, const TypedKey& fullComputedKey) override;

    bool selectIndex(const Key& idxKey, const TypedKey& fullComputedKey) override;

    bool selectDatum(const TypedKey& datumKey, const TypedKey& fullComputedKey) override;

    virtual void values(const metkit::mars::MarsRequest& request,
                        const std::string& keyword,
                        const TypesRegistry& registry,
                        eckit::StringList& values) override;

    void print( std::ostream &out ) const override;

    const Schema& databaseSchema() const override;

private:

    DB* db_;

    const Notifier& wind_;

    eckit::CacheLRU<Key,DB*>& databases_;

    InspectIterator& iterator_;

    Config config_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
