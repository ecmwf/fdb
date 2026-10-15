// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   RetrieveVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <memory>

#include "fdb5/database/ReadVisitor.h"

namespace fdb5 {

class HandleGatherer;

//----------------------------------------------------------------------------------------------------------------------

class RetrieveVisitor : public ReadVisitor {

public:  // methods

    explicit RetrieveVisitor(HandleGatherer& gatherer);

protected:  // methods

    // From Visitor

    bool selectDatabase(const Key& dbKey, const Key& fullKey) override;

    bool selectIndex(const Key& idxKey) override;

    bool selectDatum(const Key& datumKey, const Key& fullKey) override;

    void deselectDatabase() override;

    void print(std::ostream& out) const override;

    Store& store();

    const Schema& databaseSchema() const override;

private:

    std::unique_ptr<Store> store_{};

    HandleGatherer& gatherer_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

