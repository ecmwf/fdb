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
#include <sstream>
#include <string>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/fam/FamObject.h"
#include "eckit/io/fam/FamObjectName.h"
#include "eckit/io/fam/FamPath.h"
#include "eckit/io/fam/FamRegion.h"
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

auto FamStore::uri() const -> eckit::URI {
    return FamCommon::uri();
}

auto FamStore::exists() const -> bool {
    return FamCommon::exists();
}

auto FamStore::uriBelongs(const eckit::URI& uri) const -> bool {
    return root_.uriBelongs(uri);
}

auto FamStore::uriExists(const eckit::URI& uri) const -> bool {
    ASSERT(uriBelongs(uri));
    return eckit::FamObjectName(uri).exists();
}

size_t FamStore::flush() {
    LOG_DEBUG_LIB(LibFdb5) << "FamStore::flush() nothing to do!" << '\n';
    auto archived   = stats_.archived;
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
        ASSERT(uriBelongs(uri));
        res.insert(uri);
    }
    return res;
}

std::vector<eckit::URI> FamStore::getAuxiliaryURIs(const eckit::URI& uri, bool /*onlyExisting*/) const {
    ASSERT(uri.scheme() == type());
    return {};
}

auto FamStore::makeObject(const Key& key) const -> eckit::FamObjectName {
    const auto object_name = toString(key) + "-data" + std::to_string(stats_.archived);
    return root_.object(object_name).withUUID();
}

auto FamStore::retrieve(Field& field) const -> eckit::DataHandle* {
    stats_.retrieved++;
    return field.dataHandle();
}

auto FamStore::archive(const Key& key, const void* data, eckit::Length length) -> std::unique_ptr<const FieldLocation> {
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
        root_.object(eckit::FamPath(uri).objectName).lookup().deallocate();
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
            msg << "FamStore::finaliseWipeState: Index to be deleted has pointers to fields that don't belong to the "
                   "configured store."
                << '\n';
            msg << "Configured Store URI: " << this->uri().asString() << '\n';
            msg << "Pointed Store unit URI: " << uri.asString() << '\n';
            msg << "Impossible to delete such fields. Index deletion aborted to avoid leaking fields." << '\n';
            throw eckit::SeriousBug(msg.str(), Here());
        }

        if (!uriExists(uri)) {
            storeState.markAsMissing(uri);
        }
    }
}

bool FamStore::doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const {
    // if (!wipeDoit_ || !wipeUnsafeWipeAll_) {
    //     return true;
    // }

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
    const bool wipeall = wipeState.safeURIs().empty();

    for (const auto& uri : wipeState.dataAuxiliaryURIs()) {
        remove(uri, eckit::Log::info(), eckit::Log::info(), true);
    }

    for (const auto& uri : wipeState.includedDataURIs()) {
        remove(uri, eckit::Log::info(), eckit::Log::info(), true);
    }

    if (wipeall) {
        cleanupEmptyDatabase_ = true;
    }

    return true;
}

void FamStore::doWipeEmptyDatabase() const {
    if (!cleanupEmptyDatabase_) {
        return;
    }

    if (root_.exists()) {
        root_.lookup().destroy();
    }
    cleanupEmptyDatabase_ = false;
}

void FamStore::print(std::ostream& out) const {
    out << "FamStore[root=" << root_ << ']';
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
