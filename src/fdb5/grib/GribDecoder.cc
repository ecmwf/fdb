/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>
#include <cctype>

#include "eccodes.h"

#include "eckit/config/Resource.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

#include "metkit/mars/TypeAny.h"

#include "fdb5/grib/GribDecoder.h"

#include "metkit/data/Reader.h"
#include "metkit/data/Message.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static eckit::Mutex local_mutex;

GribDecoder::GribDecoder(bool checkDuplicates):
    checkDuplicates_(checkDuplicates) {
}

GribDecoder::~GribDecoder() {}

class HandleDeleter {
    grib_handle *h_;
public:
    HandleDeleter(grib_handle *h) : h_(h) {}
    ~HandleDeleter() {
        if (h_) {
            grib_handle_delete(h_);
        }
    }

    codes_handle* get() { return h_; }

};

void GribDecoder::gribToKey(const metkit::data::Message& msg, Key &key) {

    static std::string gribToRequestNamespace = eckit::Resource<std::string>("gribToRequestNamespace", "mars");

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);


    // The key must be clean at this point, as it is being returned (MARS-689)
    key.clear();

    /// @todo this code should be factored out into metkit

    ASSERT(msg);

    HandleDeleter patched(patch(msg.codesHandle()));

    const codes_handle* h = patched.get() ? patched.get() : msg.codesHandle();



    grib_keys_iterator *ks = grib_keys_iterator_new(
                                 const_cast<codes_handle*>(h),
                                 GRIB_KEYS_ITERATOR_ALL_KEYS,
                                 gribToRequestNamespace.c_str());

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

    if (key.find("param") != key.end()) {
        char value[1024];
        size_t len = sizeof(value);
        if (grib_get_string(h, "paramId", value, &len) == 0) {
            key.set("param", value);
        }
    }

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

metkit::mars::MarsRequest GribDecoder::gribToRequest(const eckit::PathName &path, const char *verb) {
    metkit::mars::MarsRequest r(verb);

    metkit::data::Reader reader(path);
    metkit::data::Message msg;

    Key key;

    std::map<std::string, std::set<std::string> > s;

    while ( (msg = reader.next())  ) {

        gribToKey(msg, key);

        for (Key::const_iterator j = key.begin(); j != key.end(); ++j) {
            s[j->first].insert(j->second);
        }
    }

    for (std::map<std::string, std::set<std::string> >::const_iterator j = s.begin(); j != s.end(); ++j) {
        eckit::StringList v(j->second.begin(), j->second.end());

        // When deserialising requests, metkit uses Type Any. So we should
        // use that here to. This could probably be done better?
        r.setValuesTyped(new metkit::mars::TypeAny(j->first), v);
    }

    return r;
}


std::vector<metkit::mars::MarsRequest> GribDecoder::gribToRequests(const eckit::PathName &path, const char *verb) {

    std::vector<metkit::mars::MarsRequest> requests;
    metkit::data::Reader reader(path);
    metkit::data::Message msg;

    Key key;

    std::map<std::string, std::set<std::string> > s;

    while ( (msg = reader.next()) ) {

        gribToKey(msg, key);

        metkit::mars::MarsRequest r(verb);
        for (Key::const_iterator j = key.begin(); j != key.end(); ++j) {
            eckit::StringList s;
            s.push_back(j->second);

            // When deserialising requests, metkit uses Type Any. So we should
            // use that here to. This could probably be done better?
            r.setValuesTyped(new metkit::mars::TypeAny(j->first), s);
        }

        requests.push_back(r);
    }

    return requests;
}

grib_handle* GribDecoder::patch(const grib_handle*) {
    // Give a chance to subclasses to modify the grib
    return nullptr;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
