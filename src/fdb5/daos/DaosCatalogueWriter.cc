/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <limits.h>
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

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        fdb5::DaosKeyValueName catalogue_kv{pool_, db_cont_, catalogue_kv_};

        fdb5::DaosSession s{};

        /// @note: performed RPCs (only if the index wasn't visited yet, i.e. only on first write to an index key):
        /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
        /// - check if cont exists if not cached (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - generate catalogue kv oid if not previously generated (daos_obj_generate_oid) -- always performed, never previously generated as catalogue_kv is local and copies catalogue_kv_
        /// - open container if not open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - ensure catalogue kv exists (daos_obj_open) -- always performed, objects not cached for now
        fdb5::StatsTimer st{"archive 01 catalogue kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
        fdb5::DaosKeyValue catalogue_kv_obj{s, catalogue_kv};
        st.stop();

        /// @todo: use an optional, or follow a functional approach (lambda function)
        std::unique_ptr<fdb5::DaosKeyValueName> index_kv;

        /// @note: performed RPCs (only if the index wasn't visited yet, i.e. only on first write to an index key):
        /// - check if catalogue kv contains index key (daos_kv_get without a buffer) -- always performed
        st.start("archive 02 catalogue kv check", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
        if (!catalogue_kv_obj.has(key.valuesToString())) {
            st.stop();

            firstIndexWrite_ = true;

            /// create index kv
            /// @todo: pass oclass from config
            /// @todo: hash string into lower oid bits
            fdb5::DaosKeyValueOID index_kv_oid{key.valuesToString(), OC_S1};
            index_kv.reset(new fdb5::DaosKeyValueName{pool_, db_cont_, index_kv_oid});
            /// @note: performed RPCs (only if the index wasn't visited yet and index kv doesn't exist yet, i.e. only on first write to an index key):
            /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
            /// - check if database container exists if not cached/open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
            /// - create database container if not exists (daos_cont_create_with_label) -- always skipped, always exists
            /// - generate index kv oid if not previously generated (daos_obj_generate_oid) -- always performed
            /// - create/open index kv (daos_kv_open) -- always performed
            /// - close index kv when out of scope in DaosContainer::createKeyValue (daos_obj_close) -- always performed
            st.start("archive 03 index kv create", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            index_kv->create();
            st.stop();

            /// write indexKey under "key"
            eckit::MemoryHandle h{(size_t) PATH_MAX};
            eckit::HandleStream hs{h};
            h.openForWrite(eckit::Length(0));
            {
                eckit::AutoClose closer(h);
                hs << currentIndexKey_;
            }
            /// @note: performed RPCs (only if the index wasn't visited yet and index kv doesn't exist yet, i.e. only on first write to an index key):
            /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
            /// - check if cont exists if not cached (daos_cont_open) -- always skipped, always cached/open after selectDatabase
            /// - generate index kv oid if not previously generated (daos_obj_generate_oid) -- always skipped, already generated in index_kv->create()
            /// - open container if not open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
            /// - ensure catalogue kv exists (daos_obj_open) -- always performed, objects not cached for now. Should be cached, or previous index_kv->create() skipped altogether
            /// - record index key into index kv (daos_kv_put) -- always performed
            /// - close index kv when destroyed (daos_obj_close) -- always performed
            st.start("archive 04 index kv put key", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            fdb5::DaosKeyValue{s, *index_kv}.put("key", h.data(), hs.bytesWritten());
            st.stop();

            /// index index kv in catalogue kv
            std::string nstr{index_kv->URI().asString()};
            /// @note: performed RPCs (only if the index wasn't visited yet and index kv doesn't exist yet, i.e. only on first write to an index key):
            /// - record index kv location into catalogue kv (daos_kv_put) -- always performed
            st.start("archive 05 catalogue kv put index", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            catalogue_kv_obj.put(key.valuesToString(), nstr.data(), nstr.length());
            st.stop();

        } else {
            st.stop();

            /// @note: performed RPCs (only if the index wasn't visited yet and index kv exists, i.e. only on first write to an existing index key):
            /// - retrieve index kv location from catalogue kv (daos_kv_get without a buffer + daos_kv_get) -- always performed
            st.start("archive 06 catalogue kv get index", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            daos_size_t size{catalogue_kv_obj.size(key.valuesToString())};
            std::vector<char> n((long) size);
            catalogue_kv_obj.get(key.valuesToString(), &n[0], size);
            st.stop();
            index_kv.reset(new fdb5::DaosKeyValueName{eckit::URI{std::string{n.begin(), n.end()}}});

        }

        indexes_[key] = Index(new fdb5::DaosIndex(key, *index_kv));

        /// @note: performed RPCs (only if the index wasn't visited yet, i.e. only on first write to an index key):
        /// - close catalogue kv (daos_obj_close) -- always performed
        st.start("archive 07 catalogue kv close", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

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

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    /// @note: the current index timestamp is undefined at this point
    Field field(fieldLocation, currentIndex().timestamp());

    /// @todo: is sorting axes really necessary?
    /// @note: sort in-memory axis values. Not triggering retrieval from DAOS axes.
    const_cast<fdb5::IndexAxis&>(current_.axes()).sort();

    /// before in-memory axes are updated as part of current_.put, we determine which
    /// additions will need to be performed on axes in DAOS after the field gets indexed.
    std::vector<std::string> axesToExpand;
    std::vector<std::string> valuesToAdd;
    std::string axisNames = "";
    std::string sep = "";

    for (Key::const_iterator i = key.begin(); i != key.end(); ++i) {

        const std::string &keyword = i->first;

        axisNames += sep + keyword;
        sep = ",";

        std::string value = key.canonicalValue(keyword);

        /// @note: obtain in-memory axis values. Not triggering retrieval from DAOS axes.
        const eckit::DenseSet<std::string>& axis_set = current_.axes().valuesNothrow(keyword);

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

        /// @note: performed RPCs (only if the index wasn't visited yet, i.e. only on first write to an index key):
        /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
        /// - check if cont exists if not cached (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - generate index kv oid if not previously generated (daos_obj_generate_oid) -- always performed
        /// - open container if not open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - ensure index kv exists (daos_obj_open) -- always performed, objects not cached for now
        fdb5::StatsTimer st{"archive 13 index kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
        fdb5::DaosKeyValue kv{s, n};
        st.stop();

        /// @note: performed RPCs:
        /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
        /// - check if cont exists if not cached (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - open container if not open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - ensure index kv exists (daos_obj_open) -- always performed, objects not cached for now. Should be cached
        /// - record axis names into index kv (daos_kv_put) -- always performed
        /// - close index kv when destroyed (daos_obj_close) -- always performed
        st.start("archive 14 index kv put field location", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
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
        /// @note: performed RPCs:
        /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
        /// - check if cont exists if not cached (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - generate axis kv oid if not previously generated (daos_obj_generate_oid) -- always performed
        /// - open container if not open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - ensure axis kv exists (daos_obj_open) -- always performed, objects not cached for now
        fdb5::StatsTimer st{"archive 15 axis kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
        fdb5::DaosKeyValue kv{s, n};
        st.stop();

        std::string v{"1"};

        /// @note: performed RPCs:
        /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
        /// - check if cont exists if not cached (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - open container if not open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
        /// - ensure axis kv exists (daos_obj_open) -- always performed, objects not cached for now. Should be cached
        /// - record axis value into axis kv (daos_kv_put) -- always performed
        /// - close axis kv when destroyed (daos_obj_close) -- always performed
        st.start("archive 16 axis kv put", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
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
