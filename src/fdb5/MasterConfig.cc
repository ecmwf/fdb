/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"
#include "eckit/config/ResourceMgr.h"
#include "eckit/parser/StringTools.h"
#include "eckit/runtime/Context.h"

#include "marslib/MarsTask.h"

#include "fdb5/MasterConfig.h"
#include "fdb5/DB.h"
#include "fdb5/Key.h"
#include "fdb5/Rule.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MasterConfig::MasterConfig()
{
    char* home = ::getenv("DHSHOME");
    if(home) {
        eckit::Context::instance().home(home);
    }

    ResourceMgr::instance().appendConfig("~/etc/config/fdb");

    rules_.load( eckit::Resource<PathName>("fdbRules", "~/etc/config/fdbRules") );

    rules_.dump(std::cout);
}

MasterConfig::~MasterConfig()
{
}

const Rules& MasterConfig::rules() const
{
    return rules_;
}

Key MasterConfig::makeDBKey(const Key& key) const
{
    const Rule* r = rules_.match(key, 0);

    ASSERT(r); /// @todo add meaningful error handling here

    StringList keys = r->keys(0);

    Log::info() << ">>>>>>>>>>>>> " << keys << std::endl;

    return key.subkey(keys);
}

MasterConfig& MasterConfig::instance()
{
    static MasterConfig master;
    return master;
}

eckit::SharedPtr<DB> MasterConfig::openSessionDB(const Key& user)
{
    Key dbKey = makeDBKey(user);

    std::string fdbWriterDB = eckit::Resource<std::string>("fdbWriterDB","toc.writer");
    return SharedPtr<DB>( DBFactory::build(fdbWriterDB, dbKey) );
}

const KeywordHandler& MasterConfig::lookupHandler(const std::string& keyword) const {
    return handlers_.lookupHandler(keyword);
}

// VecDB MasterConfig::openSessionDBs(const MarsTask& task)
// {
//     std::string fdbReaderDB = eckit::Resource<std::string>("fdbReaderDB","toc.reader");

//     VecDB result;

//     struct Collector : public Visitor {
//         std::set<Key> keySet_;
//         virtual void collect(const fdb5::Key& key,
//                              const fdb5::Key&,
//                              const fdb5::Key&) {
//             keySet_.insert(key);
//         }
//     };

//     Collector c;

//     rules_.expand(task.request(), c);

//     for(std::set<Key>::const_iterator i = c.keySet_.begin(); i != c.keySet_.end(); ++i) {
//         SharedPtr<DB> newDB ( DBFactory::build(fdbReaderDB, *i) );
//         if(newDB->open()) {
//             result.push_back( newDB );
//         }
//     }

//     return result;
// }

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
