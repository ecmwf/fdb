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
#include <cctype>

#include "grib_api.h"

#include "eckit/serialisation/MemoryStream.h"

#include "marslib/EmosFile.h"
#include "fdb5/grib/GribDecoder.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GribDecoder::GribDecoder(bool checkDuplicates):
    buffer_(80 * 1024 * 1024),
    checkDuplicates_(checkDuplicates) {
}

class HandleDeleter {
    grib_handle *h_;
public:
    HandleDeleter(grib_handle *h) : h_(h) {}
    ~HandleDeleter() {
        grib_handle_delete(h_);
    }
};

size_t GribDecoder::gribToKey(EmosFile &file, Key &key) {

    size_t len;

    if ( (len = file.readSome(buffer_)) ) {
        ASSERT(len < buffer_.size());


        const char *p = buffer_ + len - 4;

        if (p[0] != '7' || p[1] != '7' || p[2] != '7' || p[3] != '7')
            throw eckit::SeriousBug("No 7777 found");

        grib_handle *h = grib_handle_new_from_message(0, buffer_, len);
        ASSERT(h);
        HandleDeleter del(h);

        patch(h);

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
            /// const Type& type = Type::lookup(name);
            //  request.set( name, type.cannocalise(val) );
            key.set( name, val );
        }

        grib_keys_iterator_delete(ks);

        // {
        //     char value[1024];
        //     size_t len = sizeof(value);
        //     if(grib_get_string(h, "paramId", value, &len) == 0) {
        //         key.set("param", value);
        //     }
        // }

        // Look for request embbeded in GRIB message
        long local;
        size_t size;
        if (grib_get_long(h, "localDefinitionNumber", &local) ==  0 && local == 191) {
            /* TODO: Not grib2 compatible, but speed-up process */
            if (grib_get_size(h, "freeFormData", &size) ==  0 && size != 0) {
                unsigned char buffer[size];
                ASSERT(grib_get_bytes(h, "freeFormData", buffer, &size) == 0);

                eckit::MemoryStream s(buffer, size);

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
                    std::transform(keyword.begin(), keyword.end(), keyword.begin(), tolower);
                    s >> n; // Number of values
                    ASSERT(n == 1);
                    s >> value;
                    std::transform(value.begin(), value.end(), value.begin(), tolower);
                    key.set(keyword, value);
                }
            }
        }

        // check for duplicated entries (within same request)

        if ( checkDuplicates_ ) {
            if ( seen_.find(key) != seen_.end() ) {
                std::ostringstream oss;
                oss << "GRIB sent to FDB has duplicated parameters : " << key;
                throw eckit::SeriousBug( oss.str() );
            }

            seen_.insert(key);
        }

    }

    return len;
}

MarsRequest GribDecoder::gribToRequest(const eckit::PathName &path, const char *verb) {
    MarsRequest r(verb);

    EmosFile file(path);
    size_t len = 0;

    Key key;

    std::map<std::string, std::set<std::string> > s;

    while ( (len = gribToKey(file, key))  ) {
        for (Key::const_iterator j = key.begin(); j != key.end(); ++j) {
            s[j->first].insert(j->second);
        }
    }

    for (std::map<std::string, std::set<std::string> >::const_iterator j = s.begin(); j != s.end(); ++j) {
        eckit::StringList v(j->second.begin(), j->second.end());
        r.setValues(j->first, v);
    }

    return r;
}


void GribDecoder::patch(grib_handle*) {
    // Give a chance to subclasses to modify the grib
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
