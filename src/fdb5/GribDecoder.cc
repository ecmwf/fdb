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
#include "fdb5/GribDecoder.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GribDecoder::GribDecoder():
    buffer_(80 * 1024 * 1024) {
}

size_t GribDecoder::gribToKey(EmosFile &file, Key &key) {

    size_t len;

    if ( (len = file.readSome(buffer_)) ) {
        ASSERT(len < buffer_.size());


        const char *p = buffer_ + len - 4;

        if (p[0] != '7' || p[1] != '7' || p[2] != '7' || p[3] != '7')
            throw eckit::SeriousBug("No 7777 found");

        grib_handle *h = grib_handle_new_from_message(0, buffer_, len);
        ASSERT(h);

        char mars_str [] = "mars";
        grib_keys_iterator *ks = grib_keys_iterator_new(h, GRIB_KEYS_ITERATOR_ALL_KEYS, mars_str);
        ASSERT(ks);

        while (grib_keys_iterator_next(ks)) {
            const char *name = grib_keys_iterator_get_name(ks);

            if (name[0] == '_') continue; // skip silly underscores in GRIB

            char val[1024];
            size_t len = sizeof(val);

            ASSERT( grib_keys_iterator_get_string(ks, val, &len) == 0);

            /// @todo cannocicalisation of values
            /// const KeywordHandler& handler = KeywordHandler::lookup(name);
            //  request.set( name, handler.cannocalise(val) );
            key.set( name, val );
        }

        grib_keys_iterator_delete(ks);

        // Look for request embbeded in GRIB message
        long local;
        size_t size;
        if (grib_get_long(h, "localDefinitionNumber", &local) ==  0 && local == 191) {
            /* TODO: Not grib2 compatible, but speed-up process */
            if (grib_get_size(h, "freeFormData", &size) ==  0 && size != 0) {
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
                for (int i = 0; i < count; i++) {
                    std::string keyword, value;
                    int n;
                    s >> keyword;
                    s >> n; // Number of values
                    ASSERT(n == 1);
                    s >> value;
                    key.set(keyword, value);
                }
            }
        }
    }

    return len;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
