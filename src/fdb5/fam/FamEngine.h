/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamEngine.h
/// @author Metin Cakircali
/// @date   Jul 2024

#include "fdb5/database/Engine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FamEngine: public fdb5::Engine {
protected:  // methods
    virtual std::string name() const override;

    virtual std::string dbType() const override;

    virtual eckit::URI location(const Key& key, const Config& config) const override;

    virtual bool canHandle(const eckit::URI&, const Config& config) const override;

    virtual std::vector<eckit::URI> allLocations(const Key& key, const Config& config) const override;

    virtual std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const override;
    virtual std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq,
                                                       const Config&                    config) const override;

    virtual std::vector<eckit::URI> writableLocations(const Key& key, const Config& config) const override;

    virtual void print(std::ostream& out) const override;

private:  // methods
    std::set<eckit::PathName> databases(const std::set<Key>&                keys,
                                        const std::vector<eckit::PathName>& dirs,
                                        const Config&                       config) const;

    std::vector<eckit::URI> databases(const Key& key, const std::vector<eckit::PathName>& dirs, const Config& config) const;

    std::vector<eckit::URI> databases(const metkit::mars::MarsRequest&    rq,
                                      const std::vector<eckit::PathName>& dirs,
                                      const Config&                       config) const;

    void scan_dbs(const std::string& path, std::list<std::string>& dbs) const;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
