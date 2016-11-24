/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/io/DataHandle.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/legacy/LegacyRetriever.h"
#include "fdb5/database/ArchiveVisitor.h"

#include "marslib/MarsTask.h"

namespace fdb5 {
namespace legacy {

//----------------------------------------------------------------------------------------------------------------------

LegacyRetriever::LegacyRetriever() :
    Retriever(),
    translator_(),
    legacy_() {
}

void LegacyRetriever::retrieve(void* buffer, size_t length)
{
    MarsRequest r;

    for(Key::const_iterator itr = legacy_.begin(); itr != legacy_.end(); ++itr) {
        const std::string& keyword = itr->first;
        const std::string& value   = itr->second;
        r.setValue(keyword, value);
    }

    MarsRequest e("environ");

    MarsTask task(r, e);

    eckit::ScopedPtr<DataHandle> dh ( this->Retriever::retrieve(task) );

    dh->openForRead();  AutoClose closer(*dh);

    long len = dh->read(buffer, length);

    if(len < 0)
        throw eckit::ReadError("LegacyRetriever error reading from FDB5");
}

void LegacyRetriever::legacy(const std::string &keyword, const std::string &value) {
    translator_.set(legacy_, keyword, value);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
