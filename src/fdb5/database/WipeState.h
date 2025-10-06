/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

 #pragma once

#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/toc/TocCatalogue.h"

class Catalogue;
class Index;

namespace fdb5 {

class WipeState {

public:
    WipeState();

    WipeElements& wipeElements() { return wipeElements_; }
    const std::set<eckit::URI>& includeURIs() const { return includeURIs_; }
    const std::set<eckit::URI>& excludeURIs() const { return excludeURIs_; }
    const Catalogue& catalogue() const { return catalogue_; }

    void include(const eckit::URI& uri) {
        includeURIs_.insert(uri);
    }

    void exclude(const eckit::URI& uri) {
        excludeURIs_.insert(uri);
    }

protected:

    // We need to tell the catalogue to doit().
    const Catalogue& catalogue_;

    WipeElements wipeElements_;
    std::set<eckit::URI> includeURIs_;
    std::set<eckit::URI> excludeURIs_;
};



class TocWipeState: public WipeState {
public:
    

private:

    friend class TocCatalogue;

    std::set<eckit::URI> subtocPaths_         = {};
    std::set<eckit::PathName> lockfilePaths_  = {};
    std::set<eckit::URI> indexPaths_          = {};
    std::set<eckit::URI> safePaths_           = {};
    std::set<eckit::PathName> residualPaths_  = {};
    std::vector<Index> indexesToMask_         = {};
    std::set<eckit::PathName> cataloguePaths_ = {};

};

} // namespace fdb5