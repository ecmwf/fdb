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

#include "eckit/io/fam/FamObject.h"
#include "eckit/log/Log.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/fam/FamFieldLocation.h"

namespace fdb5 {

static const StoreBuilder<FamStore> builder(FamCommon::TYPE);

//----------------------------------------------------------------------------------------------------------------------

FamStore::FamStore(const Schema& schema, const Key& key, const Config& config):
    Store(schema), FamCommon(config, key), config_(config) { }

FamStore::~FamStore() = default;

auto FamStore::uri() const -> eckit::URI {
    return root().uri();
}

bool FamStore::uriBelongs(const eckit::URI& uri) const {
    return root().uriBelongs(uri);
}

bool FamStore::uriExists(const eckit::URI& uri) const {
    ASSERT(uri.endpoint() == root().endpoint());
    return eckit::FamObjectName(uri).exists();
}

std::set<eckit::URI> FamStore::asCollocatedDataURIs(const std::vector<eckit::URI>& /* uriList */) const {
    NOTIMP;
}

std::vector<eckit::URI> FamStore::collocatedDataURIs() const {
    NOTIMP;
}

bool FamStore::exists() const {
    return root().exists();
}

eckit::FamObjectName FamStore::makeObject(const Key& key) const {
    const auto objectName = toString(key);
    return root().object(objectName).withUUID();
}

eckit::DataHandle* FamStore::retrieve(Field& field) const {
    return field.dataHandle();
}

std::unique_ptr<FieldLocation> FamStore::archive(const Key& key, const void* data, eckit::Length length) {
    auto object = makeObject(key);

    LOG_DEBUG_LIB(LibFdb5) << "FamStore::archive() => " << object << '\n';

    {
        auto handle = std::unique_ptr<eckit::DataHandle>(object.dataHandle());

        handle->openForWrite(length);
        const eckit::AutoClose closer(*handle);

        handle->write(data, length);
    }

    return std::make_unique<FamFieldLocation>(object.uri(), 0, length, fdb5::Key());
}

void FamStore::flush() {
    NOTIMP;
}

void FamStore::close() {
    NOTIMP;
}

void FamStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {
    ASSERT(uri.endpoint() == root().endpoint());

    const auto path = eckit::FamPath(uri);  // asserts uri.scheme
    ASSERT(path.regionName == root().path().regionName);

    logVerbose << "remove: ";
    logAlways << uri << std::endl;

    if (doit) { root().object(path.objectName).lookup().deallocate(); }
}

void FamStore::print(std::ostream& out) const {
    out << "FamStore[schema=" << schema_ << ", root=" << root() << ']';
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
