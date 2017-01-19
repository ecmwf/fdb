/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <ostream>

#include "fdb5/toc/TocEngine.h"
#include "fdb5/toc/RootManager.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string TocEngine::name() const {
    return TocEngine::typeName();
}

std::string TocEngine::dbType() const {
    return TocEngine::typeName();
}

eckit::PathName TocEngine::location(const Key& key) const
{
    return RootManager::directory(key);
}

bool TocEngine::canHandle(const eckit::PathName& path) const
{
    NOTIMP;
}

std::vector<eckit::PathName> TocEngine::allLocations(const Key& key) const
{
    return RootManager::allRoots(key);
}

std::vector<eckit::PathName> TocEngine::visitableLocations(const Key& key) const
{
    return RootManager::visitableRoots(key);
}

std::vector<eckit::PathName> TocEngine::writableLocations(const Key& key) const
{
    return RootManager::writableRoots(key);
}

void TocEngine::print(std::ostream& out) const
{
    out << "TocEngine()";
}

static EngineBuilder<TocEngine> toc_builder;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
