/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/KeyBasedFileSpaceHandler.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/thread/AutoLock.h"

#include "fdb5/toc/FileSpace.h"
#include "fdb5/database/Key.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

KeyBasedFileSpaceHandler::KeyBasedFileSpaceHandler() {
}

KeyBasedFileSpaceHandler::~KeyBasedFileSpaceHandler() {
}

void KeyBasedFileSpaceHandler::load() const {

    PathName keyToFileSpaceDefinitions = Resource<PathName>("keyToFileSpaceDefinitions", "~fdb/share/fdb/ExpverFileSpace.cfg");

    std::ifstream in(keyToFileSpaceDefinitions.localPath());

    if(!in) {
        std::ostringstream oss;
        oss <<  keyToFileSpaceDefinitions << Log::syserr;
        Log::error() << oss.str() << std::endl;
        throw CantOpenFile(oss.str(), Here());
    }

    char line[1024];
    size_t lineNo = 0;
    Tokenizer parse(" ");
    std::vector<std::string> s;

    while(in.getline(line, sizeof(line)))
    {
        ++lineNo;
        s.clear();

        parse(line,s);

        size_t i = 0;
        while( i < s.size() ) /* cleanup entries that are empty */
        {
            if(s[i].length() == 0)
                s.erase(s.begin()+i);
            else
                i++;
        }

        if(s.size() == 0 || s[0][0] == '#')
            continue;

        if(s.size() != 2) {
            std::ostringstream oss;
            oss << "Bad line (" << lineNo << ") in configuration file " << keyToFileSpaceDefinitions << " -- should have format 'key value'";
            throw ReadError(oss.str(), Here());
        }

        table_[ s[0] ] = PathName(s[1]);
    }
}

void KeyBasedFileSpaceHandler::append(const std::string& expver, const PathName& path) const
{
    NOTIMP;
}

PathName KeyBasedFileSpaceHandler::select(const std::string& expver) const
{
    NOTIMP;
}

eckit::PathName KeyBasedFileSpaceHandler::selectFileSystem(const Key& key, const FileSpace& fs) const {

    AutoLock<Mutex> lock(mutex_);

    // check if key is mapped already to a filesystem

    if(table_.empty()) load();

    std::string expver = key.get("expver");

    PathTable::const_iterator itr = table_.find(expver);
    if(itr != table_.end()) {
        return itr->second;
    }

    // if not, assign a filesystem

    PathName selected = select(expver);

    table_[expver] = selected;

    append(expver, selected);

    return selected;
}

FileSpaceHandlerRegister<KeyBasedFileSpaceHandler> expver("expver");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
