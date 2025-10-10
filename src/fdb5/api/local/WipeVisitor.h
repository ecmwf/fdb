/*
 * (C) Copyright 2018- ECMWF.
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

#pragma once

#include <unordered_map>

#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/local/QueryVisitor.h"


template <>
struct std::hash<eckit::URI> {
    std::size_t operator()(const eckit::URI& uri) const noexcept {
        const std::string& e = uri.asRawString();
        return std::hash<std::string>{}(e);
    }
};

namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

struct StoreURIs {
    std::unique_ptr<Store> store;
    std::set<eckit::URI> dataURIs;
    std::set<eckit::URI> safeURIs;
};

//----------------------------------------------------------------------------------------------------------------------

class WipeVisitor : public QueryVisitor<WipeElement> {

public:  // methods

    WipeVisitor(eckit::Queue<WipeElement>& queue, const metkit::mars::MarsRequest& request, bool doit, bool porcelain,
                bool unsafeWipeAll);

    bool visitEntries() override { return false; }
    bool visitDatabase(const Catalogue& catalogue) override;
    bool visitIndex(const Index& index) override;
    void catalogueComplete(const Catalogue& catalogue) override;

    // These methods are not used in the wipe visitor
    void visitDatum(const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }
    void visitDatum(const Field& /*field*/, const std::string& /*keyFingerprint*/) override { NOTIMP; }

    void onDatabaseNotFound(const fdb5::DatabaseNotFoundException& e) override { throw e; }

private:  // methods

    void storeURI(const eckit::URI& dataURI, bool include);

private:  // members

    bool doit_;
    bool porcelain_;
    bool unsafeWipeAll_;

    metkit::mars::MarsRequest indexRequest_;

    std::unordered_map<eckit::URI, StoreURIs> stores_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local
