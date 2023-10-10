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

#ifndef fdb5_toc_TocEngine_H
#define fdb5_toc_TocEngine_H

#include "fdb5/database/Engine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TocEngine : public fdb5::Engine {

public: // methods

    static const char* typeName() { return "toc"; }

private:  // methods
    std::set<eckit::PathName> databases(const std::set<InspectionKey>& keys, const std::vector<eckit::PathName>& dirs,
                                        const Config& config) const;

    std::vector<eckit::URI> databases(const Key& key, const std::vector<eckit::PathName>& dirs, const Config& config) const;

    std::vector<eckit::URI> databases(const metkit::mars::MarsRequest& rq, const std::vector<eckit::PathName>& dirs,
                                      const Config& config) const;

    void scan_dbs(const std::string& path, std::list<std::string>& dbs) const;

protected: // methods

    virtual std::string name() const override;

    virtual std::string dbType() const override;

    virtual eckit::URI location(const Key &key, const Config& config) const override;

    virtual bool canHandle(const eckit::URI& path) const override;

    virtual std::vector<eckit::URI> allLocations(const Key& key, const Config& config) const override;

    virtual std::vector<eckit::URI> visitableLocations(const Config& config) const override;
    virtual std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq, const Config& config) const override;

//    virtual std::vector<eckit::URI> writableLocations(const InspectionKey& key, const Config& config) const override;

    virtual void print( std::ostream &out ) const override;

};

//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5

#endif
