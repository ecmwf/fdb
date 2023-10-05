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

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    fdb5::DaosSession s{};

    /// @note: performed RPCs:
    /// - daos_pool_connect
    /// - root cont open (daos_cont_open)
    /// - root cont create (daos_cont_create)
    fdb5::StatsTimer st{"archive 001 ensure pool and root container", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosPool& p = s.getPool(pool_);
    p.ensureContainer(root_cont_);
    st.stop();

    fdb5::DaosKeyValueName main_kv_name{pool_, root_cont_, main_kv_};

    /// @note: the DaosKeyValue constructor checks if the kv exists, which results in creation if not exists
    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)
    st.start("archive 002 main kv ensure", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
    fdb5::DaosKeyValue main_kv{s, main_kv_name};
    st.stop();

    fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};

    /// @note: performed RPCs:
    /// - check if main kv contains db key (daos_kv_get without a buffer)
    st.start("archive 003 main kv check if db exists", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
    if (!main_kv.has(db_cont_)) {
        st.stop();

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

        int db_key_max_len = 512;  // @todo: take from config
        if (hs.bytesWritten() > db_key_max_len)
            throw eckit::Exception("Serialised db key exceeded configured maximum db key length.");
        
        fdb5::DaosKeyValue{s, catalogue_kv_name}.put("key", h.data(), hs.bytesWritten());   

        /// index newly created catalogue kv in main kv
        int db_loc_max_len = 512;  // @todo: take from config
        std::string nstr = catalogue_kv_name.URI().asString();
        if (nstr.length() > db_loc_max_len) 
            throw eckit::Exception("Serialised db location exceeded configured maximum db location length.");

        main_kv.put(db_cont_, nstr.data(), nstr.length());

    }
    st.stop();
    
    /// @todo: record or read dbUID

    /// @note: performed RPCs:
    /// - catalogue container open (daos_cont_open)
    /// - get schema from catalogue kv (daos_kv_get)
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

        /// @note: performed RPCs:
        /// - generate catalogue kv oid (daos_obj_generate_oid)
        /// - ensure catalogue kv exists (daos_kv_open)
        fdb5::StatsTimer st{"archive 01 catalogue kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
        fdb5::DaosKeyValue catalogue_kv_obj{s, catalogue_kv};
        st.stop();

        /// @todo: use an optional, or follow a functional approach (lambda function)
        std::unique_ptr<fdb5::DaosKeyValueName> index_kv;

        int idx_loc_max_len = 512;  /// @todo: take from config

        try {

            std::vector<char> n((long) idx_loc_max_len);
            long res;

            /// @note: performed RPCs:
            /// - get index location from catalogue kv (daos_kv_get)
            st.start("archive 02 catalogue kv attempt get index location", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            res = catalogue_kv_obj.get(key.valuesToString(), &n[0], idx_loc_max_len);
            st.stop();

            index_kv.reset(new fdb5::DaosKeyValueName{eckit::URI{std::string{n.begin(), std::next(n.begin(), res)}}});

        } catch (fdb5::DaosEntityNotFoundException& e) {

            st.stop();

            firstIndexWrite_ = true;

            /// create index kv
            /// @todo: pass oclass from config
            /// @todo: hash string into lower oid bits
            fdb5::DaosKeyValueOID index_kv_oid{key.valuesToString(), OC_S1};
            index_kv.reset(new fdb5::DaosKeyValueName{pool_, db_cont_, index_kv_oid});

            /// @note: performed RPCs:
            /// - generate index kv oid (daos_obj_generate_oid)
            /// - create/open index kv (daos_kv_open)
            st.start("archive 03 index kv open/create", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            fdb5::DaosKeyValue index_kv_obj{s, *index_kv};
            st.stop();

            /// write indexKey under "key"
            eckit::MemoryHandle h{(size_t) PATH_MAX};
            eckit::HandleStream hs{h};
            h.openForWrite(eckit::Length(0));
            {
                eckit::AutoClose closer(h);
                hs << currentIndexKey_;
            }

            int idx_key_max_len = 512;

            if (hs.bytesWritten() > idx_key_max_len)
                throw eckit::Exception("Serialised index key exceeded configured maximum index key length.");

            /// @note: performed RPCs:
            /// - record index key into index kv (daos_kv_put)
            st.start("archive 04 index kv put key", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            index_kv_obj.put("key", h.data(), hs.bytesWritten());
            st.stop();

            /// index index kv in catalogue kv
            std::string nstr{index_kv->URI().asString()};
            if (nstr.length() > idx_loc_max_len)
                throw eckit::Exception("Serialised index location exceeded configured maximum index location length.");
            /// @note: performed RPCs (only if the index wasn't visited yet and index kv doesn't exist yet, i.e. only on first write to an index key):
            /// - record index kv location into catalogue kv (daos_kv_put) -- always performed
            st.start("archive 05 catalogue kv put index", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            catalogue_kv_obj.put(key.valuesToString(), nstr.data(), nstr.length());
            st.stop();

            /// @note: performed RPCs:
            /// - close index kv when destroyed (daos_obj_close)
            st.start("archive 06 catalogue kv close", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

        }
        st.stop();

        indexes_[key] = Index(new fdb5::DaosIndex(key, *index_kv));

        /// @note: performed RPCs:
        /// - close catalogue kv (daos_obj_close)
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

        /// @note: performed RPCs:
        /// - generate index kv oid (daos_obj_generate_oid)
        /// - ensure index kv exists (daos_obj_open)
        fdb5::StatsTimer st{"archive 13 index kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
        fdb5::DaosKeyValue kv{s, n};
        st.stop();

        int axis_names_max_len = 512;
        if (axisNames.length() > axis_names_max_len)
            throw eckit::Exception("Serialised axis names exceeded configured maximum axis names length.");

        /// @note: performed RPCs:
        /// - record axis names into index kv (daos_kv_put)
        /// - close index kv when destroyed (daos_obj_close)
        st.start("archive 14 index kv put axis names", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
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

        /// @note: performed RPCs:
        /// - generate axis kv oid (daos_obj_generate_oid)
        /// - ensure axis kv exists (daos_obj_open)
        fdb5::StatsTimer st{"archive 15 axis kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
        fdb5::DaosKeyValue kv{s, n};
        st.stop();

        std::string v{"1"};

        /// @note: performed RPCs:
        /// - record axis value into axis kv (daos_kv_put)
        /// - close axis kv when destroyed (daos_obj_close)
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
