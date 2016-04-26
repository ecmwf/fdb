/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Progress.h"

#include "marslib/EmosFile.h"

#include "fdb5/GribArchiver.h"
#include "fdb5/ArchiveVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GribArchiver::GribArchiver(bool completeTransfers) :
    Archiver(),
    GribDecoder(),
    completeTransfers_(completeTransfers)
{
}

eckit::Length GribArchiver::archive(eckit::DataHandle& source)
{
    eckit::Timer timer("fdb::service::archive");

    EmosFile file(source);
    size_t len = 0;

    size_t count = 0;
    size_t total_size = 0;

    eckit::Progress progress("FDB archive", 0, file.length());

    try{

        Key key;

        while( (len = gribToKey(file, key)) )
        {
            ArchiveVisitor visitor(*this, key, static_cast<const void *>(buffer()), len);

            this->Archiver::archive(key, visitor);

            total_size += len;
            count++;
            progress(total_size);
        }
    }
    catch(...) {

        if(completeTransfers_) {
            eckit::Log::error() << "Exception recieved. Completing transfer." << std::endl;
            // Consume rest of datahandle otherwise client retries for ever
            eckit::Buffer buffer(80*1024*1024);
            while( (len = file.readSome(buffer)) )
                /* empty */;
        }
        throw;
    }

    eckit::Log::info() << "FDB archive " << eckit::Plural(count, "field") << ","
                       << " size " << eckit::Bytes(total_size) << ","
                       << " in " << eckit::Seconds(timer.elapsed())
                       << " (" << eckit::Bytes(total_size, timer) << ")" <<  std::endl;

    return total_size;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
