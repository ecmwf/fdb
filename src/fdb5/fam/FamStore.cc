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

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamFieldLocation.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/fam/FamObject.h"
#include "eckit/io/fam/FamObjectName.h"
#include "eckit/io/fam/FamPath.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace fdb5 {

static const StoreBuilder<FamStore> builder(FamCommon::type);

//----------------------------------------------------------------------------------------------------------------------

FamStore::FamStore(const Key& key, const Config& config) : FamCommon(key, config) {}

FamStore::FamStore(const eckit::URI& uri, const Config& config) : FamCommon(uri, config) {}

FamStore::~FamStore() = default;

//----------------------------------------------------------------------------------------------------------------------

eckit::URI FamStore::uri(const eckit::URI& data_uri) {
    ASSERT(data_uri.scheme() == FamCommon::type);
    return eckit::FamObjectName(data_uri).uri();
}

eckit::URI FamStore::uri() const {
    return FamCommon::uri();
}

bool FamStore::exists() const {
    return FamCommon::exists();
}

bool FamStore::uriBelongs(const eckit::URI& uri) const {
    return FamCommon::uriBelongs(uri);
}

bool FamStore::uriExists(const eckit::URI& uri) const {
    ASSERT(uriBelongs(uri));
    return eckit::FamObjectName(uri).exists();
}

size_t FamStore::flush() {
    LOG_DEBUG_LIB(LibFdb5) << "FamStore::flush() nothing to do!" << '\n';
    return stats_.archived.exchange(0);
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

eckit::FamObject& FamStore::counter() const {
    std::call_once(countOnce_, [this] { counter_.emplace(getRegion().ensureObject(sizeof(uint64_t), counter_name)); });
    return *counter_;
}

eckit::FamObjectName FamStore::makeObject(const Key& key) const {
    // concurrent-safe id
    const auto id = counter().fetchAdd<uint64_t>(0, 1);
    // derive a deterministic UUID from the full path (region + object name)
    const auto object_name = toString(key) + "-data" + std::to_string(id);
    return root().object(object_name).withUUID();
}

eckit::DataHandle* FamStore::retrieve(Field& field) const {
    stats_.retrieved.fetch_add(1, std::memory_order_relaxed);
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

    stats_.archived.fetch_add(1, std::memory_order_relaxed);

    return std::make_unique<FamFieldLocation>(object.uri(), 0, length, fdb5::Key());
}

void FamStore::remove(const eckit::URI& uri, std::ostream& log_always, std::ostream& log_verbose, bool doit) const {
    ASSERT(root().uriBelongs(uri));

    log_verbose << "remove: ";
    log_always << uri << '\n';

    if (doit) {
        try {
            root().object(eckit::FamPath(uri).objectName()).lookup().deallocate();
        }
        catch (const eckit::NotFound&) {
            LOG_DEBUG_LIB(LibFdb5) << "FamStore::remove: object already absent: " << uri << '\n';
        }
    }
}

void FamStore::finaliseWipeState(StoreWipeState& store_state, bool /*doit*/, bool /*unsafe_wipe_all*/) {

    const std::set<eckit::URI>& data_ur_is = store_state.includedDataURIs();

    if (data_ur_is.empty()) {
        return;
    }

    if (!root().exists()) {
        store_state.markAllMissing();
        return;
    }

    for (const auto& uri : data_ur_is) {
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
            store_state.markAsMissing(uri);
        }
    }
}

bool FamStore::doWipeUnknowns(const std::set<eckit::URI>& unknown_ur_is) const {
    for (const auto& uri : unknown_ur_is) {
        if (!uriBelongs(uri)) {
            continue;
        }
        if (uriExists(uri)) {
            remove(uri, eckit::Log::info(), eckit::Log::info(), true);
        }
    }
    return true;
}

bool FamStore::doWipeURIs(const StoreWipeState& wipe_state) const {
    for (const auto& uri : wipe_state.dataAuxiliaryURIs()) {
        remove(uri, eckit::Log::info(), eckit::Log::info(), true);
    }
    for (const auto& uri : wipe_state.includedDataURIs()) {
        remove(uri, eckit::Log::info(), eckit::Log::info(), true);
    }
    // TODO: when all URIs are consumed and the DB is empty, destroy the FAM region
    // (doWipeEmptyDatabase). Requires auditing the region-per-DB layout assumption first.
    return true;
}

void FamStore::doWipeEmptyDatabase() const {
    // TODO: destroy the root FAM region once it is confirmed that each DB has its own
    // dedicated region. If multiple DBs share a single region (as may happen with some
    // FAM configurations), calling root().lookup().destroy() would silently delete
    // neighbour data. Implement only after the region-per-DB layout is enforced.
}

void FamStore::print(std::ostream& out) const {
    out << "FamStore[root=" << root() << ']';
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
