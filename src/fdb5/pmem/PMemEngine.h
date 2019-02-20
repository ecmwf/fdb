/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Jan 2017

#ifndef fdb5_pmem_PMemEngine_H
#define fdb5_pmem_PMemEngine_H

#include "fdb5/database/Engine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class PMemEngine : public fdb5::Engine {

public: // methods

    static const char* typeName() { return "pmem"; }

    static std::vector<eckit::PathName> databases(const Key& key,
                                                  const std::vector<eckit::PathName>& dirs,
                                                  const Config& config);

    static std::vector<eckit::PathName> databases(const metkit::MarsRequest& request,
                                                  const std::vector<eckit::PathName>& dirs,
                                                  const Config& config);

private: // methods

    static std::vector<eckit::PathName> databases(const std::set<Key>& keys,
                                                  const std::vector<eckit::PathName>& dirs,
                                                  const Config& config);

protected: // methods

    virtual std::string name() const;

    virtual std::string dbType() const;

    virtual eckit::PathName location(const Key &key, const Config& config) const;

    virtual bool canHandle(const eckit::PathName& path) const;

    virtual std::vector<eckit::PathName> allLocations(const Key& key, const Config& config) const;

    virtual std::vector<eckit::PathName> visitableLocations(const Key& key, const Config& config) const;
    virtual std::vector<eckit::PathName> visitableLocations(const metkit::MarsRequest& rq, const Config& config) const;

    virtual std::vector<eckit::PathName> writableLocations(const Key& key, const Config& config) const;

    virtual void print( std::ostream &out ) const;

};

//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5

#endif
