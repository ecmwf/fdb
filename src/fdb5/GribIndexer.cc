/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/DataHandle.h"
#include "eckit/log/Timer.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Progress.h"

#include "grib_api.h"

#include "marslib/EmosFile.h"

#include "fdb5/Key.h"
#include "fdb5/GribIndexer.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GribIndexer::GribIndexer(bool checkDuplicates) :
    checkDuplicates_(checkDuplicates)
{
}

void GribIndexer::index(const eckit::PathName& path)
{
    Timer timer("fdb::service::archive");
    double check = 0;

    EmosFile file(path);
    size_t len = 0;

    std::set<Key> seen;

    double tbegin = timer.elapsed();

    size_t count = 0;
    Length total_size = 0;

    Length totalFileSize = path.size();

    Progress progress("Scanning", 0, totalFileSize);

    Key request;


    while( (len = gribToKey(file, request))  )
    {

        // check for duplicated entries (within same request)
        if(checkDuplicates_)
        {
            double now = timer.elapsed();
            if( seen.find(request) != seen.end() )
            {
                std::ostringstream msg;
                msg << "GRIB sent to FDB has duplicated parameters : " << request;
                Log::error() << msg.str() << std::endl;
            }

            seen.insert(request);
            ++count;

            check += timer.elapsed() - now;
        }

        Log::info() << request << std::endl;

        Length length = len;
        Offset offset = file.position() - length;

        adopt(request, path, offset, length); // now index it

        total_size += len;
        progress(total_size);
    }


    double tend = timer.elapsed();
    double ttotal = tend-tbegin;

    Log::info() << "FDB indexer " << BigNum(count) << " fields,"
                << " size " << Bytes(total_size) << ","
                << " in " << Seconds(ttotal)
                << " (" << Bytes(total_size,ttotal) << ")" <<  std::endl;

    if(checkDuplicates_) {
        Log::info() << "Time spent in checking for duplicates on " << BigNum(count)
                    << " took " << Seconds(check) << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
