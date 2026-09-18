/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_EntryVisitMechanism_H
#define fdb5_EntryVisitMechanism_H

#include "fdb5/config/Config.h"
#include "fdb5/database/DatabaseNotFoundException.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/OrderedParallelFor.h"

#include <memory>
#include <mutex>

namespace eckit {
class URI;
}

namespace fdb5 {

class Catalogue;
class Store;
class FDBToolRequest;
class Index;
class Rule;
class Key;

//----------------------------------------------------------------------------------------------------------------------

class EntryVisitor {

public:  // types

    /// In the EntryVisitMechanism indexes can be visited in parallel. So the visitEntries
    /// function needs a way to know what is the current index, and a way to store/accumulate
    /// index-specific values.
    ///
    /// This class may be subclassed to include anything else which is needed per-index
    /// when running in parallel (e.g. a specific output queue)
    class IndexScope {

    public:  // methods

        IndexScope(const Catalogue& catalogue, const Index& index, const Rule& rule) :
            catalogue_(catalogue), index_(index), rule_(rule) {}
        virtual ~IndexScope() = default;

        IndexScope(const IndexScope&) = delete;
        IndexScope& operator=(const IndexScope&) = delete;

        const Catalogue& catalogue() const { return catalogue_; }
        const Index& index() const { return index_; }
        const Rule& rule() const { return rule_; }

    private:  // members

        const Catalogue& catalogue_;
        const Index& index_;
        const Rule& rule_;
    };

    using IndexScopePtr = std::unique_ptr<IndexScope>;

public:  // methods

    EntryVisitor();

    EntryVisitor(const EntryVisitor&) = delete;
    EntryVisitor& operator=(const EntryVisitor&) = delete;
    EntryVisitor(EntryVisitor&&) = delete;
    EntryVisitor& operator=(EntryVisitor&&) = delete;

    virtual ~EntryVisitor();

    // defaults
    virtual bool visitIndexes() { return true; }
    virtual bool visitEntries() { return true; }

    virtual bool preVisitDatabase(const eckit::URI& uri, const Schema& schema);
    virtual bool visitDatabase(const Catalogue& catalogue);  // return true if Catalogue should be explored
    virtual void catalogueComplete(const Catalogue& catalogue);

    virtual IndexScopePtr visitIndex(const Index& index, OrderedParallelFor::Order& order);
    virtual IndexScopePtr visitIndex(const Index& index, const Rule& rule);

    virtual void visitDatum(IndexScope& scope, const Field& field, const std::string& keyFingerprint);

    virtual void onDatabaseNotFound(const fdb5::DatabaseNotFoundException& e) {}

    virtual bool supportsConcurrentIndexVisitation() const { return false; }

protected:

    Store& store() const;
    virtual void visitDatum(IndexScope& scope, const Field& field, const Key& datumKey) = 0;

    /// The datum rule for an index of the database currently being visited.
    const Rule& indexRule(const Index& index) const;

protected:  // members

    /// Non-owning
    const Catalogue* currentCatalogue_{nullptr};

    // Owned Store
    mutable Store* currentStore_{nullptr};
    mutable std::mutex storeMutex_;
};

//----------------------------------------------------------------------------------------------------------------------

class EntryVisitMechanism {

public:  // methods

    EntryVisitMechanism(const Config& config);

    EntryVisitMechanism(const EntryVisitMechanism&) = delete;
    EntryVisitMechanism& operator=(const EntryVisitMechanism&) = delete;
    EntryVisitMechanism(EntryVisitMechanism&&) = delete;
    EntryVisitMechanism& operator=(EntryVisitMechanism&&) = delete;

    void visit(const FDBToolRequest& request, EntryVisitor& visitor);

private:  // members

    const Config& dbConfig_;

    // Fail on error
    bool fail_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
