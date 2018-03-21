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
/// @date   March 2018

#ifndef fdb5_dist_DistEngine_H
#define fdb5_dist_DistEngine_H

#include "fdb5/database/Engine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DistEngine : public Engine {

public: // methods

    static const char* typeName() { return "dist"; }

    static std::vector<eckit::PathName> databases(const Key& key, const std::vector<eckit::PathName>& dirs);

protected: // methods

    virtual std::string name() const;

    virtual std::string dbType() const;

    virtual eckit::PathName location(const Key &key) const;

    virtual bool canHandle(const eckit::PathName& path) const;

    virtual std::vector<eckit::PathName> allLocations(const Key& key) const;

    virtual std::vector<eckit::PathName> visitableLocations(const Key& key) const;

    virtual std::vector<eckit::PathName> writableLocations(const Key& key) const;

    virtual void print( std::ostream &out ) const;

};

//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5

#endif // fdb5_dist_DistEngine_H
