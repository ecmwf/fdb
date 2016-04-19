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
#include "eckit/serialisation/HandleStream.h"
#include "eckit/io/MemoryHandle.h"
#include "grib_api.h"

#include "marslib/EmosFile.h"
#include "marslib/MarsRequest.h"

#include "fdb5/Key.h"
#include "fdb5/GribArchiver.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GribArchiver::GribArchiver(bool completeTransfers):
    completeTransfers_(completeTransfers)
{
}

Length GribArchiver::archive(eckit::DataHandle& source)
{
    Timer timer("fdb::service::archive");
    double check = 0;

    EmosFile file(source);
    size_t len = 0;

    std::set<Key> seen;

    double tbegin = timer.elapsed();

    size_t count = 0;
    size_t total_size = 0;

    // Length totalEstimate = source.estimate();

    try{

        Key key;

        while( (len = gribToKey(file, key)) )
        {

            // check for duplicated entries (within same request)

    //        if( fdbCheckDoubleGrib_ )
            {
                double now = timer.elapsed();
                if( seen.find(key) != seen.end() )
                {
                    std::ostringstream msg;
                    msg << "GRIB sent to FDB has duplicated parameters : " << key;
                    Log::error() << msg.str() << std::endl;

    //                if( fdbFailOnOverwrite_ )
    //                    throw fdb::Error( Here(), msg.str() );
                }

                seen.insert(key);
                ++count;

                check += timer.elapsed() - now;
            }

            Log::info() << key << std::endl;

            write(key, static_cast<const void *>(buffer()), len ); // finally archive it

            total_size += len;
        }
    }
    catch(...) {
        if(completeTransfers_) {
            Log::error() << "Exception recieved. Completing transfer." << std::endl;
            // Consume rest of datahandle otherwise client retries for ever
            eckit::Buffer buffer(80*1024*1024);
            while( (len = file.readSome(buffer)) )
                /* empty */;
        }
        throw;
    }

    double tend = timer.elapsed();
    double ttotal = tend-tbegin;

    Log::info() << "FDB archive " << BigNum(count) << " fields,"
                << " size " << Bytes(total_size) << ","
                << " in " << Seconds(ttotal)
                << " (" << Bytes(total_size,ttotal) << ")" <<  std::endl;

    Log::info() << "Time spent in checking for duplicates on " << BigNum(count)
                << " took " << Seconds(check) << std::endl;

    return total_size;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
