/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"
#include "eckit/parser/StringTools.h"

#include "fdb5/MasterConfig.h"
#include "fdb5/DB.h"
#include "fdb5/Key.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MasterConfig::MasterConfig()
{

    fdbIgnoreOnOverwrite_ = eckit::Resource<bool>("fdbIgnoreOnOverwrite", true);
    fdbWarnOnOverwrite_   = eckit::Resource<bool>("fdbWarnOnOverwrite",   true);
    fdbFailOnOverwrite_   = eckit::Resource<bool>("fdbFailOnOverwrite",   false);
    fdbAllKeyChecks_      = eckit::Resource<bool>("fdbAllKeyChecks",      true);
    fdbCheckAcceptable_   = eckit::Resource<bool>("fdbCheckAcceptable",   true);
    fdbCheckRequired_     = eckit::Resource<bool>("fdbCheckRequired",     true);

    std::string masterDBKeysStr = eckit::Resource<std::string>("masterDBKeys","{class}:{stream}:{expver}:{date}");
    masterDBKeys_ = StringTools::substituteVariables( masterDBKeysStr );
}

MasterConfig::~MasterConfig()
{
}

Key MasterConfig::makeDBKey(const Key& key) const
{
    return key.subkey(masterDBKeys_);
}

MasterConfig& MasterConfig::instance()
{
    static MasterConfig master;
    return master;
}

eckit::SharedPtr<DB> MasterConfig::openSessionDB(const Key& user)
{
    Key dbKey = makeDBKey(user);

    return SharedPtr<DB>( DBFactory::build("toc.write", dbKey) );
}

VecDB MasterConfig::openSessionDBs(const MarsTask& task)
{
    VecDB result;

    std::vector<Key> dbKeys;

    /// @todo EXPANDS task into masterDBKeys_

    NOTIMP;

    /// @todo substitute "toc" with a configuration driven DB type

    for( std::vector<Key>::const_iterator it = dbKeys.begin(); it != dbKeys.end(); ++it ) {
        result.push_back( SharedPtr<DB>( DBFactory::build("toc.read", *it) ) );
    }

    return result;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
