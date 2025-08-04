/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @author Emanuele Danovaro
/// @date   November 2018

#ifndef fdb5_api_WipeIterator_H
#define fdb5_api_WipeIterator_H

#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/APIIterator.h"

#include <set>
#include <string>

namespace eckit {
class Stream;
}

/*
 * Define a standard object which can be used to iterate the results of a
 * wipe() call on an arbitrary FDB object
 */

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

enum WipeElementType {
    WIPE_ERROR,
    WIPE_CATALOGUE_INFO,
    WIPE_CATALOGUE,
    WIPE_CATALOGUE_SAFE,
    WIPE_CATALOGUE_AUX,
    WIPE_STORE_INFO,
    WIPE_STORE_URI,
    WIPE_STORE,
    WIPE_STORE_SAFE,
    WIPE_STORE_AUX,
    WIPE_UNKNOWN
};

class WipeElement {
public:  // methods

    WipeElement() = default;
    WipeElement(WipeElementType type, const std::string& msg);
    WipeElement(WipeElementType type, const std::string& msg, eckit::URI uri);
    WipeElement(WipeElementType type, const std::string& msg, std::set<eckit::URI>&& uris);
    explicit WipeElement(eckit::Stream& s);

    void print(std::ostream& out) const;
    size_t encodeSize() const;

    WipeElementType type() const { return type_; }
    const std::string& msg() const { return msg_; }
    const std::set<eckit::URI>& uris() const { return uris_; }

    void add(const eckit::URI& uri) { uris_.insert(uri); }

private:  // methods

    void encode(eckit::Stream& s) const;

    friend std::ostream& operator<<(std::ostream& os, const WipeElement& e) {
        e.print(os);
        return os;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeElement& r) {
        r.encode(s);
        return s;
    }

private:  // members

    WipeElementType type_;
    std::string msg_;
    std::set<eckit::URI> uris_;
};

using WipeElements = std::vector<std::shared_ptr<WipeElement>>;

using WipeIterator          = APIIterator<WipeElement>;
using WipeAggregateIterator = APIAggregateIterator<WipeElement>;
using WipeAsyncIterator     = APIAsyncIterator<WipeElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
