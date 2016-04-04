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

#include "fdb5/MasterConfig.h"
#include "fdb5/TocSchema.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocSchema::TocSchema(const Key& dbKey) :
    dbKey_(dbKey)
{
    root_ = eckit::Resource<std::string>("fdbRoot;$FDB_ROOT","/tmp/fdb");
    root_.mkdir(); /// @note what about permissions on creation?

    dirPath_ = root_ / MasterConfig::instance().makeDBPath(dbKey);

    indexType_ = eckit::Resource<std::string>( "fdbIndexType", "BTreeIndex" );

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
    Log::info() << userKey << std::endl;
    std::string tocKeys = eckit::Resource<std::string>("tocKeys","{type}:{levtype}", userKey.dict());
    return StringTools::substitute(tocKeys, userKey.dict());
}

Index::Key TocSchema::dataIdx(const Key& userKey) const
{
    Tokenizer parse(",");
    eckit::StringList idxKeys;

    Index::Key subdict;

    std::string s = eckit::Resource<std::string>("idxKeys","levelist,step,param", userKey.dict());
    parse(s,idxKeys);

    const StringDict& params = userKey.dict();
    for( eckit::StringList::const_iterator i = idxKeys.begin(); i != idxKeys.end(); ++i )
    {
        StringDict::const_iterator itr = params.find(*i);
        ASSERT( itr != params.end() );
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
