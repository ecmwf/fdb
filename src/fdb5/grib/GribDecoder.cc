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

#include "metkit/mars/TypeAny.h"
#include "fdb5/grib/GribDecoder.h"
#include "fdb5/database/KeySetter.h"

#include "eckit/message/Reader.h"
#include "eckit/message/Message.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GribDecoder::GribDecoder(bool checkDuplicates):
    checkDuplicates_(checkDuplicates) {
}

GribDecoder::~GribDecoder() {}

void GribDecoder::gribToKey(const eckit::message::Message& msg, Key &key) {

    eckit::message::Message patched = patch(msg);

    KeySetter setter(key);
    patched.getMetadata(setter);

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

    eckit::message::Reader reader(path);
    eckit::message::Message msg;

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
    eckit::message::Reader reader(path);
    eckit::message::Message msg;

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

eckit::message::Message GribDecoder::patch(const eckit::message::Message& msg) {
    // Give a chance to subclasses to modify the grib
    return msg;
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
