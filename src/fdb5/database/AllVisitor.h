/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   AllVisitor.h
/// @author Emanuele Danovaro
/// @author Simon Smart
/// @author Tiago Quintino
/// @date   Aug 2020

#ifndef fdb5_AllVisitor_H
#define fdb5_AllVisitor_H

#include <memory>

#include "fdb5/database/Key.h"
#include "fdb5/database/ReadVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class AllVisitor : public ReadVisitor {

public: // methods

    AllVisitor(std::vector<Key>* keys, const Schema& schema);

    ~AllVisitor();


private:  // methods

    // From Visitor

    bool selectDatabase(const Key &key, const Key &full) override;
    bool selectIndex(const Key &key, const Key &full) override;
    bool selectDatum(const Key &key, const Key &full) override;

    void values(const metkit::mars::MarsRequest& request,
                        const std::string& keyword,
                        const TypesRegistry& registry,
                        eckit::StringList& values) override;

    void print( std::ostream &out ) const override;

    const Schema& databaseSchema() const override;

private:

    const Schema& schema_;
    std::vector<Key>* keys_;
    Key k0_;
    Key k1_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
