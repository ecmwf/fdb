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


#include "fdb5/database/ReadVisitor.h"
#include "eckit/memory/ScopedPtr.h"


namespace fdb5 {

class HandleGatherer;
class Notifier;

class DB;

//----------------------------------------------------------------------------------------------------------------------

class RetrieveVisitor : public ReadVisitor {

public: // methods

    RetrieveVisitor(const Notifier &wind, HandleGatherer &gatherer);

    ~RetrieveVisitor();


private:  // methods

    // From Visitor

    virtual bool selectDatabase(const Key &key, const Key &full);

    virtual bool selectIndex(const Key &key, const Key &full);

    virtual bool selectDatum(const Key &key, const Key &full);

    virtual void values(const metkit::MarsRequest& request,
                        const std::string& keyword,
                        const TypesRegistry& registry,
                        eckit::StringList& values);

    virtual void print( std::ostream &out ) const;

private:

    const Notifier &wind_;

    eckit::ScopedPtr<DB> db_;

    HandleGatherer &gatherer_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
