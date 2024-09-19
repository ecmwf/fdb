/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   RetrieveVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_RetrieveVisitor_H
#define fdb5_RetrieveVisitor_H

#include <memory>

#include "fdb5/database/ReadVisitor.h"

namespace fdb5 {

class HandleGatherer;
class Notifier;

//----------------------------------------------------------------------------------------------------------------------

class RetrieveVisitor : public ReadVisitor {

public:  // methods

    RetrieveVisitor(const Notifier& wind, HandleGatherer& gatherer);

protected:  // methods

    // From Visitor

    bool selectDatabase(const Key& dbKey, const Key& fullKey) override;

    bool selectIndex(const Key& idxKey, const Key& fullKey) override;

    bool selectDatum(const Key& datumKey, const Key& fullKey) override;

    void values(const metkit::mars::MarsRequest& request, const std::string& keyword, const TypesRegistry& registry,
                eckit::StringList& values) override;

    void print(std::ostream& out) const override;

    Store& store();
    const Schema& databaseSchema() const override;

private:

    std::unique_ptr<Store> store_;

    const Notifier& wind_;

    HandleGatherer& gatherer_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
