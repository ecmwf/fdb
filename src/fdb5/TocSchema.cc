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
#include "eckit/parser/Tokenizer.h"
#include "eckit/parser/StringTools.h"

#include "fdb5/Error.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/TocSchema.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocSchema::TocSchema(const Key& dbKey) :
    dbKey_(dbKey)
{
    root_ = eckit::Resource<std::string>("fdbRoot;$FDB_ROOT", "/tmp/fdb", dbKey.dict());

    root_.mkdir(); /// @note what about permissions on creation?

    dirPath_ = root_ / MasterConfig::instance().makeDBPath(dbKey);

    indexType_ = eckit::Resource<std::string>( "fdbIndexType", "BTreeIndex", dbKey.dict());

    /// @todo the schema should be generated as we walk the expansion (not a predifned one as below)

    push_back("class");
    push_back("stream");
    push_back("expver");
    push_back("type");
    push_back("levtype");
    push_back("date");
    push_back("time");
    push_back("param");
    push_back("step");
    push_back("levelist");
}


TocSchema::~TocSchema()
{
}

bool TocSchema::matchTOC(const Key& tocKey) const
{
    return tocKey.match(dbKey_);
}

eckit::PathName TocSchema::tocDirPath() const
{
    return dirPath_;
}

std::string TocSchema::dataFilePrefix(const Key& userKey) const
{
    std::string dataKeys = eckit::Resource<std::string>("dataKeys","{type}.{levtype}.{step}", userKey.dict());
    return StringTools::substitute(dataKeys, userKey.dict());
}

eckit::PathName TocSchema::generateIndexPath(const Key& userKey) const
{
    PathName tocPath ( tocDirPath() );
    tocPath /= tocEntry(userKey);
    tocPath = PathName::unique(tocPath) + ".idx";
    return tocPath;
}

eckit::PathName TocSchema::generateDataPath(const Key& userKey) const
{
    PathName dpath ( tocDirPath() );
    dpath /= dataFilePrefix(userKey);
    dpath = PathName::unique(dpath) + ".dat";
    return dpath;
}

std::string TocSchema::tocEntry(const Key& userKey) const
{
    std::string tocKeys = eckit::Resource<std::string>("tocKeys","{type}:{levtype}", userKey.dict());
    return StringTools::substitute(tocKeys, userKey.dict());
}

Key TocSchema::dataIdx(const Key& userKey) const
{
    Tokenizer parse(",");
    eckit::StringList idxKeys;

    Key subdict;

    std::string s = eckit::Resource<std::string>("idxKeys","levelist,step,param", userKey.dict());

    Log::info() << "-----> idxKeys: " << idxKeys << std::endl;

    parse(s,idxKeys);

    for( eckit::StringList::const_iterator i = idxKeys.begin(); i != idxKeys.end(); ++i )
    {
        StringDict::const_iterator itr = userKey.dict().find(*i);
        if( itr == userKey.dict().end() ) {
            std::ostringstream msg;
            msg << "User request misses required key " << *i << ", request is " << userKey;
            throw fdb5::Error(Here(), msg.str());
        }
        subdict.set( *i, itr->second );
    }

    return subdict;
}

const std::string& TocSchema::indexType() const
{
    return indexType_;
}

void TocSchema::print(std::ostream& out) const
{
    out << "TocSchema("
        << "dirPath=" << dirPath_
        << ",indexType=" << indexType_
        << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
