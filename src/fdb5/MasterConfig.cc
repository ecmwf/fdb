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

#include "marslib/MarsTask.h"

#include "fdb5/MasterConfig.h"
#include "fdb5/DB.h"
#include "fdb5/Key.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MasterConfig::MasterConfig()
{
    masterDBKeysStr_ = eckit::Resource<std::string>("masterDBKeys","{class}:{stream}:{expver}:{date}:{time}");
    masterDBKeys_ = StringTools::substituteVariables( masterDBKeysStr_ );
}

MasterConfig::~MasterConfig()
{
}

std::string MasterConfig::makeDBPath(const Key& key) const
{
    return StringTools::substitute( masterDBKeysStr_, key.dict() );
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

    return SharedPtr<DB>( DBFactory::build("toc.writer", dbKey) );
}

VecDB MasterConfig::openSessionDBs(const MarsTask& task)
{
    VecDB result;

    Key dbKey;

    expand(task.request(), masterDBKeys_, 0, dbKey, result); /// @todo EXPANDS task into masterDBKeys_

    return result;
}

void MasterConfig::expand(const MarsRequest& request,
                          const std::vector<std::string>& masterKeys,
                          size_t pos,
                          Key& dbKey,
                          VecDB& result ) {


    if( pos != masterKeys.size() ) {

        StringList values;

        const std::string& name = masterKeys[pos];

        request.getValues(name, values);

        for(std::vector<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {
            dbKey.set(name, *j);
            expand(request, masterKeys, pos+1, dbKey, result);
        }
    }
    else
    {
        /// @todo substitute "toc" with a configuration driven DB type
        result.push_back( SharedPtr<DB>( DBFactory::build("toc.reader", dbKey) ) );
    }
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
