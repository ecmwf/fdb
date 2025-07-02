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
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamFieldLocation.h"

namespace fdb5 {

static const StoreBuilder<FamStore> builder(FamCommon::type);

//----------------------------------------------------------------------------------------------------------------------

FamStore::FamStore(const Key& key, const Config& config) : FamCommon(config, key), config_(config) {}

FamStore::~FamStore() = default;

//----------------------------------------------------------------------------------------------------------------------

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
    return stats_.archived;
}

void FamStore::close() {
    LOG_DEBUG_LIB(LibFdb5) << "FamStore::close() nothing to do!" << '\n';
}

auto FamStore::asCollocatedDataURIs(const std::vector<eckit::URI>& /* uriList */) const -> std::set<eckit::URI> {
    NOTIMP;
}

auto FamStore::collocatedDataURIs() const -> std::vector<eckit::URI> {
    NOTIMP;
}

auto FamStore::makeObject(const Key& key) const -> eckit::FamObjectName {
    const auto objectName = toString(key);
    return root_.object(objectName).withUUID();
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

        handle->write(data, length);
    }

    stats_.archived++;

    return std::make_unique<FamFieldLocation>(object.uri(), 0, length, fdb5::Key());
}

void FamStore::remove(const Key& key) const {
    auto object = makeObject(key);

    LOG_DEBUG_LIB(LibFdb5) << "FamStore removing object: " << object << '\n';

    object.lookup().deallocate();
}

void FamStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {
    ASSERT(root_.uriBelongs(uri));

    logVerbose << "remove: ";
    logAlways << uri << '\n';

    if (doit) {
        root_.object(eckit::FamPath(uri).objectName).lookup().deallocate();
    }
}

void FamStore::print(std::ostream& out) const {
    out << "FamStore[root=" << root_ << ']';
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
