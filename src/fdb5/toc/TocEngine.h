// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Jan 2017

#pragma once

#include "fdb5/database/Engine.h"

namespace fdb5 {

class Rule;

//----------------------------------------------------------------------------------------------------------------------

class TocEngine : public fdb5::Engine {

public:  // methods

    static const char* typeName() { return "toc"; }

private:  // methods

    std::map<eckit::PathName, const Rule*> databases(const std::map<Key, const Rule*>& keys,
                                                     const std::vector<eckit::PathName>& dirs,
                                                     const Config& config) const;

    std::vector<eckit::URI> databases(const Key& key, const std::vector<eckit::PathName>& dirs,
                                      const Config& config) const;

    std::vector<eckit::URI> databases(const metkit::mars::MarsRequest& rq, const std::vector<eckit::PathName>& dirs,
                                      const Config& config) const;

    void scan_dbs(const std::string& path, std::list<std::string>& dbs) const;

protected:  // methods

    std::string name() const override;

    std::string dbType() const override;

    eckit::URI location(const Key& key, const Config& config) const override;

    bool canHandle(const eckit::URI&, const Config& config) const override;

    std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const override;
    std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq,
                                               const Config& config) const override;

    void print(std::ostream& out) const override;
};

//----------------------------------------------------------------------------------------------------------------------


}  // namespace fdb5

