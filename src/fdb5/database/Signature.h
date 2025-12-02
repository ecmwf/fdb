/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <string>
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"

// -----------------------------------------------------------------------------------------------

namespace fdb5 {

/// @todo: Dummy placeholder for signing. We need to work on this later (e.g. use GPG or some standard library) in a
/// followup PR. The Catalogue Server signs the wipe states before sending them to clients, which the client will
/// forward to the stores. The stores will verify the signatures before proceeding with the wipe.
class Signature {

public:

    static uint64_t hashURIs(const std::set<eckit::URI>& uris, const std::string& secret) {
        uint64_t h = 0;
        std::vector<std::string> sortedURIs;
        for (const auto& uri : uris) {
            sortedURIs.push_back(uri.asRawString());
        }
        std::sort(sortedURIs.begin(), sortedURIs.end());

        for (const auto& uri : sortedURIs) {
            h ^= std::hash<std::string>{}(uri) + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        h ^= std::hash<std::string>{}(secret) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }

    Signature() {}

    Signature(eckit::Stream& s) {
        unsigned long long in;
        s >> in;
        sig_ = in;
    }

    void sign(uint64_t sig) {
        ASSERT(sig != 0);
        sig_ = sig;
    }

    bool isSigned() const { return sig_ != 0; }

    friend eckit::Stream& operator<<(eckit::Stream& s, const Signature& sig) {
        // pending patch in eckit to support uint64_t directly
        s << static_cast<unsigned long long>(sig.sig_);
        return s;
    }

    bool validSignature(uint64_t expected) const { return sig_ == expected; }

public:

    uint64_t sig_{0};
};

}  // namespace fdb5