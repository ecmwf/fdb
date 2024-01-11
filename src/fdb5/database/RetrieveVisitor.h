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

class DB;

//----------------------------------------------------------------------------------------------------------------------

class RetrieveVisitor : public ReadVisitor {

public: // methods

    RetrieveVisitor(const Notifier &wind, HandleGatherer &gatherer);

    ~RetrieveVisitor();


protected:  // methods

    // From Visitor

    virtual bool selectDatabase(const Key &key, const Key &full) override;

    virtual bool selectIndex(const Key &key, const Key &full) override;

    virtual bool selectDatum(const Key &key, const Key &full) override;

    virtual void values(const metkit::mars::MarsRequest& request,
                        const std::string& keyword,
                        const TypesRegistry& registry,
                        eckit::StringList& values) override;

    virtual void print( std::ostream &out ) const override;

    virtual const Schema& databaseSchema() const override;

protected:

    const Notifier &wind_;

    std::unique_ptr<DB> db_;

    HandleGatherer &gatherer_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
