// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   MultiRetrieveVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   October 2016

#pragma once

#include <string>

#include "eckit/container/CacheLRU.h"
#include "eckit/container/Queue.h"

#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Inspector.h"
#include "fdb5/database/ReadVisitor.h"

namespace fdb5 {

class HandleGatherer;

//----------------------------------------------------------------------------------------------------------------------

class MultiRetrieveVisitor : public ReadVisitor {

public:  // methods

    MultiRetrieveVisitor(InspectIterator& queue, eckit::CacheLRU<Key, CatalogueReader*>& databases,
                         const Config& config);

    ~MultiRetrieveVisitor();

private:  // methods

    // From Visitor

    bool selectDatabase(const Key& dbKey, const Key& fullKey) override;

    bool selectIndex(const Key& idxKey) override;

    bool selectDatum(const Key& datumKey, const Key& fullKey) override;

    void deselectDatabase() override;

    void print(std::ostream& out) const override;

    const Schema& databaseSchema() const override;

private:

    eckit::CacheLRU<Key, CatalogueReader*>& databases_;

    InspectIterator& iterator_;

    Config config_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

