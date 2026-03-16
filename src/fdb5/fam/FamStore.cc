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
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamStore.h"

#include <cstddef>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/fam/FamObject.h"
#include "eckit/io/fam/FamObjectName.h"
#include "eckit/io/fam/FamPath.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamFieldLocation.h"

namespace fdb5 {

static const StoreBuilder<FamStore> builder(FamCommon::type);

//----------------------------------------------------------------------------------------------------------------------

FamStore::FamStore(const Key& key, const Config& config) : FamCommon(key, config) {}

FamStore::FamStore(const eckit::URI& uri, const Config& config) : FamCommon(uri, config) {}

FamStore::~FamStore() = default;

//----------------------------------------------------------------------------------------------------------------------

eckit::URI FamStore::uri(const eckit::URI& dataURI) {
    ASSERT(dataURI.scheme() == FamCommon::type);
    return eckit::FamObjectName(dataURI).uri();
}

eckit::URI FamStore::uri() const {
    return FamCommon::uri();
}

bool FamStore::exists() const {
    return FamCommon::exists();
}

bool FamStore::uriBelongs(const eckit::URI& uri) const {
    return root_.uriBelongs(uri);
}

bool FamStore::uriExists(const eckit::URI& uri) const {
    ASSERT(uriBelongs(uri));
    return eckit::FamObjectName(uri).exists();
}

size_t FamStore::flush() {
    LOG_DEBUG_LIB(LibFdb5) << "FamStore::flush() nothing to do!" << '\n';
    auto archived = stats_.archived;
    stats_.archived = 0;
    return archived;
}

void FamStore::close() {
    LOG_DEBUG_LIB(LibFdb5) << "FamStore::close() nothing to do!" << '\n';
}

std::set<eckit::URI> FamStore::collocatedDataURIs() const {
    return {};
}

std::set<eckit::URI> FamStore::asCollocatedDataURIs(const std::set<eckit::URI>& uris) const {
    std::set<eckit::URI> res;
    for (const auto& uri : uris) {
        if (!uriBelongs(uri)) {
            throw eckit::BadValue("FamStore: URI does not belong to this store: " + uri.asString());
        }
        res.insert(uri);
    }
    return res;
}

std::vector<eckit::URI> FamStore::getAuxiliaryURIs(const eckit::URI& uri, bool /*onlyExisting*/) const {
    ASSERT(uri.scheme() == type());
    return {};
}

eckit::FamObjectName FamStore::makeObject(const Key& key) const {
    // The human-readable stem is used only as a debugging hint; withUUID() replaces
    // the region+object path entirely with a UUID, so the stem is not persisted.
    const auto object_name = toString(key) + "-data" + std::to_string(stats_.archived);
    return root_.object(object_name).withUUID();
}

eckit::DataHandle* FamStore::retrieve(Field& field) const {
    stats_.retrieved++;
    return field.dataHandle();
}

std::unique_ptr<const FieldLocation> FamStore::archive(const Key& key, const void* data, eckit::Length length) {
    auto object = makeObject(key);

    LOG_DEBUG_LIB(LibFdb5) << "FamStore archiving object: " << object << '\n';

    {
        auto handle = std::unique_ptr<eckit::DataHandle>(object.dataHandle());

        handle->openForWrite(length);
        const eckit::AutoClose closer(*handle);

        const auto written = handle->write(data, length);
        ASSERT(written == static_cast<long>(length));
    }

    stats_.archived++;

    return std::make_unique<FamFieldLocation>(object.uri(), 0, length, fdb5::Key());
}

void FamStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {
    ASSERT(root_.uriBelongs(uri));

    logVerbose << "remove: ";
    logAlways << uri << '\n';

    if (doit) {
        const auto name = root_.object(eckit::FamPath(uri).objectName);
        if (name.exists()) {
            name.lookup().deallocate();
        }
        else {
            LOG_DEBUG_LIB(LibFdb5) << "FamStore::remove: object already absent: " << uri << '\n';
        }
    }
}

void FamStore::finaliseWipeState(StoreWipeState& storeState, bool /*doit*/, bool /*unsafeWipeAll*/) {

    const std::set<eckit::URI>& dataURIs = storeState.includedDataURIs();

    if (dataURIs.empty()) {
        return;
    }

    if (!root_.exists()) {
        storeState.markAllMissing();
        return;
    }

    for (const auto& uri : dataURIs) {
        if (!uriBelongs(uri)) {
            std::stringstream msg;
            msg << "FamStore::finaliseWipeState: index to be deleted has pointers to fields that don't belong to the "
                   "configured store.\n";
            msg << "Configured store URI: " << this->uri().asString() << '\n';
            msg << "Field URI: " << uri.asString() << '\n';
            msg << "Index deletion aborted to avoid leaking fields.";
            throw eckit::SeriousBug(msg.str(), Here());
        }

        if (!uriExists(uri)) {
            storeState.markAsMissing(uri);
        }
    }
}

bool FamStore::doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const {
    for (const auto& uri : unknownURIs) {
        if (!uriBelongs(uri)) {
            continue;
        }
        if (uriExists(uri)) {
            remove(uri, eckit::Log::info(), eckit::Log::info(), true);
        }
    }
    return true;
}

bool FamStore::doWipeURIs(const StoreWipeState& wipeState) const {
    for (const auto& uri : wipeState.dataAuxiliaryURIs()) {
        remove(uri, eckit::Log::info(), eckit::Log::info(), true);
    }
    for (const auto& uri : wipeState.includedDataURIs()) {
        remove(uri, eckit::Log::info(), eckit::Log::info(), true);
    }
    // TODO: when all URIs are consumed and the DB is empty, destroy the FAM region
    // (doWipeEmptyDatabase). Requires auditing the region-per-DB layout assumption first.
    return true;
}

void FamStore::doWipeEmptyDatabase() const {
    // TODO: destroy the root FAM region once it is confirmed that each DB has its own
    // dedicated region. If multiple DBs share a single region (as may happen with some
    // FAM configurations), calling root_.lookup().destroy() would silently delete
    // neighbour data. Implement only after the region-per-DB layout is enforced.
}

void FamStore::print(std::ostream& out) const {
    out << "FamStore[root=" << root_ << ']';
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
