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
    Buffer buffer(80*1024*1024);
    size_t len = 0;

    std::set<Key> seen;

    double tbegin = timer.elapsed();

    size_t count = 0;
    Length total_size = 0;

    Length totalEstimate = source.estimate();

    // This progress competes with the transfer progress
    //Progress progress("Scanning", 0, totalEstimate);

    try{

        while( (len = file.readSome(buffer)) )
        {
            ASSERT(len < buffer.size());

            Log::info() << "Size is " << len << std::endl;

            const char *p = buffer + len - 4;

            if(p[0] != '7' || p[1] != '7' || p[2] != '7' || p[3] != '7')
                throw eckit::SeriousBug("No 7777 found");

            //Offset where = file.position();

            //r.length_ = len;
            //r.offset_ = ((unsigned long long)where - len);

            grib_handle *h = grib_handle_new_from_message(0,buffer,len);
            ASSERT(h);

            char mars_str [] = "mars";
            grib_keys_iterator* ks = grib_keys_iterator_new(h,GRIB_KEYS_ITERATOR_ALL_KEYS, mars_str);
            ASSERT(ks);

            fdb5::Key request;

            while(grib_keys_iterator_next(ks))
            {
                const char* name = grib_keys_iterator_get_name(ks);

                if(name[0] == '_') continue; // skip silly underscores in GRIB

                char val[1024];
                size_t len = sizeof(val);

                ASSERT( grib_keys_iterator_get_string(ks, val, &len) == 0);

                /// @todo cannocicalisation of values
                /// const KeywordHandler& handler = KeywordHandler::lookup(name);
                //  request.set( name, handler.cannocalise(val) );
                request.set( name, val );
            }

            grib_keys_iterator_delete(ks);

            // Look for request embbeded in GRIB message
            long local;
            size_t size;
            if(grib_get_long(h, "localDefinitionNumber", &local) ==  0 && local == 191) {
            /* TODO: Not grib2 compatible, but speed-up process */
                if(grib_get_size(h, "freeFormData", &size) ==  0 && size != 0) {
                    unsigned char buffer[size];
                    ASSERT(grib_get_bytes(h, "freeFormData", buffer, &size) == 0);
                    eckit::MemoryHandle handle(buffer, size);
                    handle.openForRead();
                    AutoClose close(handle);
                    eckit::HandleStream s(handle);
                    int count;
                    s >> count; // Number of requests
                    ASSERT(count == 1);
                    std::string tmp;
                    s >> tmp; // verb
                    s >> count;
                    for(int i = 0; i < count; i++) {
                        std::string keyword, value;
                        int n;
                        s >> keyword;
                        s >> n; // Number of values
                        ASSERT(n == 1);
                        s >> value;
                        request.set(keyword, value);
                    }
                }
            }

            // check for duplicated entries (within same request)

    //        if( fdbCheckDoubleGrib_ )
            {
                double now = timer.elapsed();
                if( seen.find(request) != seen.end() )
                {
                    std::ostringstream msg;
                    msg << "GRIB sent to FDB has duplicated parameters : " << request;
                    Log::error() << msg.str() << std::endl;

    //                if( fdbFailOnOverwrite_ )
    //                    throw fdb::Error( Here(), msg.str() );
                }

                seen.insert(request);
                ++count;

                check += timer.elapsed() - now;
            }

            Log::info() << request << std::endl;

            write(request, static_cast<const void *>(buffer), Length(len) ); // finally archive it

            total_size += len;
            //progress(total_size);
        }
    }
    catch(...) {
        if(completeTransfers_) {
         Log::error() << "Exception recieved. Completing transfer." << std::endl;
            // Consume rest of datahandle otherwise client retries for ever
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
