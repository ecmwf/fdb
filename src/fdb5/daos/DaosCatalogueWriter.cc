/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <linux/limits.h>
#include <numeric>

#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/serialisation/HandleStream.h"

#include "fdb5/LibFdb5.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosKeyValueHandle.h"

#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosCatalogueWriter.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCatalogueWriter::DaosCatalogueWriter(const Key &key, const fdb5::Config& config) :
    DaosCatalogue(key, config), firstIndexWrite_(false) {

    fdb5::DaosName pool_name{pool_};
    ASSERT(pool_name.exists());

    fdb5::DaosKeyValueName main_kv_name{pool_, root_cont_, main_kv_};
    /// @todo: just create directly?
    if (!main_kv_name.exists()) main_kv_name.create();

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue main_kv{s, main_kv_name};
    fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};

    if (!main_kv.has(db_cont_)) {

        /// create catalogue kv
        catalogue_kv_name.create();

        /// write schema under "schema"
        eckit::Log::debug<LibFdb5>() << "Copy schema from "
                                     << config_.schemaPath()
                                     << " to "
                                     << catalogue_kv_name.URI().asString()
                                     << " at key 'schema'."
                                     << std::endl;

        eckit::FileHandle in(config_.schemaPath());
        std::unique_ptr<eckit::DataHandle> out(catalogue_kv_name.dataHandle("schema"));
        in.copyTo(*out);

        /// write dbKey under "key"
        eckit::MemoryHandle h{(size_t) PATH_MAX};
        eckit::HandleStream hs{h};
        h.openForWrite(eckit::Length(0));
        {
            eckit::AutoClose closer(h);
            hs << dbKey_;
        }
        fdb5::DaosKeyValue{s, catalogue_kv_name}.put("key", h.data(), hs.bytesWritten());   

        /// index newly created catalogue kv in main kv
        std::string nstr = catalogue_kv_name.URI().asString();
        main_kv.put(db_cont_, nstr.data(), nstr.length());

    }
    
    /// @todo: record or read dbUID

    DaosCatalogue::loadSchema();

    /// @todo: TocCatalogue::checkUID();

}

DaosCatalogueWriter::DaosCatalogueWriter(const eckit::URI &uri, const fdb5::Config& config) :
    DaosCatalogue(uri, ControlIdentifiers{}, config), firstIndexWrite_(false) {

    NOTIMP;

}

DaosCatalogueWriter::~DaosCatalogueWriter() {

    clean();
    close();

}

bool DaosCatalogueWriter::selectIndex(const Key& key) {

    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        fdb5::DaosKeyValueName catalogue_kv{pool_, db_cont_, catalogue_kv_};

        fdb5::DaosSession s{};
        fdb5::DaosKeyValue catalogue_kv_obj{s, catalogue_kv};

        /// @todo: use an optional, or follow a functional approach (lambda function)
        std::unique_ptr<fdb5::DaosKeyValueName> index_kv;

        if (!catalogue_kv.has(key.valuesToString())) {

            firstIndexWrite_ = true;

            /// create index kv
            /// @todo: pass oclass from config
            /// @todo: hash string into lower oid bits
            fdb5::DaosKeyValueOID index_kv_oid{key.valuesToString(), OC_S1};
            index_kv.reset(new fdb5::DaosKeyValueName{pool_, db_cont_, index_kv_oid});
            index_kv->create();

            /// write indexKey under "key"
            eckit::MemoryHandle h{(size_t) PATH_MAX};
            eckit::HandleStream hs{h};
            h.openForWrite(eckit::Length(0));
            {
                eckit::AutoClose closer(h);
                hs << currentIndexKey_;
            }
            fdb5::DaosKeyValue{s, *index_kv}.put("key", h.data(), hs.bytesWritten());   

            /// index index kv in catalogue kv
            std::string nstr{index_kv->URI().asString()};
            catalogue_kv_obj.put(key.valuesToString(), nstr.data(), nstr.length());

        } else {

            daos_size_t size{catalogue_kv_obj.size(key.valuesToString())};
            std::vector<char> n((long) size);
            catalogue_kv_obj.get(key.valuesToString(), &n[0], size);
            index_kv.reset(new fdb5::DaosKeyValueName{eckit::URI{std::string{n.begin(), n.end()}}});

        }

        indexes_[key] = Index(new fdb5::DaosIndex(key, *index_kv));

    }

    current_ = indexes_[key];

    return true;

}

void DaosCatalogueWriter::deselectIndex() {

    current_ = Index();
    currentIndexKey_ = Key();
    firstIndexWrite_ = false;

}

void DaosCatalogueWriter::clean() {

    flush();

    deselectIndex();

}

void DaosCatalogueWriter::close() {

    closeIndexes();

}

const Index& DaosCatalogueWriter::currentIndex() {

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    return current_;

}

/// @todo: other writers may be simultaneously updating the axes KeyValues in DAOS. Should these
///        new updates be retrieved and put into in-memory axes from time to time, e.g. every
///        time a value is put in an axis KeyValue?
void DaosCatalogueWriter::archive(const Key& key, const FieldLocation* fieldLocation) {

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    Field field(fieldLocation, currentIndex().timestamp());

    const_cast<fdb5::IndexAxis&>(current_.axes()).sort();

    /// before in-memory axes are updated as part of current_.put, we determine which
    /// additions will need to be performed on axes in DAOS after the field gets indexed.
    std::vector<std::string> axesToExpand;
    std::vector<std::string> valuesToAdd;
    std::string axisNames = "";
    std::string sep = ",";

    for (Key::const_iterator i = key.begin(); i != key.end(); ++i) {

        const std::string &keyword = i->first;

        axisNames += sep + keyword;
        sep = ",";

        std::string value = key.canonicalValue(keyword);

        const eckit::DenseSet<std::string>& axis_set = current_.axes().valuesSafe(keyword);

        if (!axis_set.contains(value)) {

            axesToExpand.push_back(keyword);
            valuesToAdd.push_back(value);

        }

    }

    /// index the field and update in-memory axes
    current_.put(key, field);

    fdb5::DaosSession s{};

    /// persist axis names
    if (firstIndexWrite_) {

        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid{currentIndexKey_.valuesToString(), OC_S1};
        fdb5::DaosKeyValueName n{pool_, db_cont_, oid};
        fdb5::DaosKeyValue kv{s, n};
        kv.put("axes", axisNames.data(), axisNames.length());

        firstIndexWrite_ = false;

    }

    /// @todo: axes are supposed to be sorted before persisting. How do we do this with the DAOS approach?
    ///        sort axes every time they are loaded in the read pathway?

    if (axesToExpand.empty()) return;

    /// expand axis info in DAOS
    while (!axesToExpand.empty()) {

        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid{currentIndexKey_.valuesToString() + std::string{"."} + axesToExpand.back(), OC_S1};
        fdb5::DaosKeyValueName n{pool_, db_cont_, oid};
        /// @note: OID is being generated here below
        /// @note: axis KV is being implicitly created here if not exists
        fdb5::DaosKeyValue kv{s, n};
        std::string v{"1"};
        kv.put(valuesToAdd.back(), v.data(), v.length());
        axesToExpand.pop_back();
        valuesToAdd.pop_back();

    }

}

void DaosCatalogueWriter::flush() {

    if (!current_.null()) current_ = Index();

}

void DaosCatalogueWriter::closeIndexes() {

    indexes_.clear(); // all indexes instances destroyed

}

static fdb5::CatalogueBuilder<fdb5::DaosCatalogueWriter> builder("daos.writer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
