/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <climits>
#include <numeric>

#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/serialisation/HandleStream.h"

#include "fdb5/LibFdb5.h"

#include "fdb5/daos/DaosKeyValueHandle.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

#include "fdb5/daos/DaosCatalogueWriter.h"
#include "fdb5/daos/DaosIndex.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCatalogueWriter::DaosCatalogueWriter(const Key& key, const fdb5::Config& config) :
    DaosCatalogue(key, config), firstIndexWrite_(false) {


    fdb5::DaosSession s{};

    /// @note: performed RPCs:
    /// - daos_pool_connect
    /// - root cont open (daos_cont_open)
    /// - root cont create (daos_cont_create)
    fdb5::DaosPool& p = s.getPool(pool_);
    p.ensureContainer(root_cont_);

    fdb5::DaosKeyValueName main_kv_name{pool_, root_cont_, main_kv_};

    /// @note: the DaosKeyValue constructor checks if the kv exists, which results in creation if not exists
    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)
    fdb5::DaosKeyValue main_kv{s, main_kv_name};

    fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};

    /// @note: performed RPCs:
    /// - check if main kv contains db key (daos_kv_get without a buffer)
    if (!main_kv.has(db_cont_)) {

        /// create catalogue kv
        catalogue_kv_name.create();

        /// write schema under "schema"
        eckit::Log::debug<LibFdb5>() << "Copy schema from " << config_.schemaPath() << " to "
                                     << catalogue_kv_name.URI().asString() << " at key 'schema'." << std::endl;

        eckit::FileHandle in(config_.schemaPath());
        std::unique_ptr<eckit::DataHandle> out(catalogue_kv_name.dataHandle("schema"));
        in.copyTo(*out);

        /// write dbKey under "key"
        eckit::MemoryHandle h{(size_t)PATH_MAX};
        eckit::HandleStream hs{h};
        h.openForWrite(eckit::Length(0));
        {
            eckit::AutoClose closer(h);
            hs << dbKey_;
        }

        int db_key_max_len = 512;  // @todo: take from config
        if (hs.bytesWritten() > db_key_max_len) {
            throw eckit::Exception("Serialised db key exceeded configured maximum db key length.");
        }

        fdb5::DaosKeyValue{s, catalogue_kv_name}.put("key", h.data(), hs.bytesWritten());

        /// index newly created catalogue kv in main kv
        int db_loc_max_len = 512;  // @todo: take from config
        std::string nstr = catalogue_kv_name.URI().asString();
        if (nstr.length() > db_loc_max_len) {
            throw eckit::Exception("Serialised db location exceeded configured maximum db location length.");
        }

        main_kv.put(db_cont_, nstr.data(), nstr.length());
    }

    /// @todo: record or read dbUID

    /// @note: performed RPCs:
    /// - catalogue container open (daos_cont_open)
    /// - get schema from catalogue kv (daos_kv_get)
    DaosCatalogue::loadSchema();

    /// @todo: TocCatalogue::checkUID();
}

DaosCatalogueWriter::DaosCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config) :
    DaosCatalogue(uri, ControlIdentifiers{}, config), firstIndexWrite_(false) {

    NOTIMP;
}

DaosCatalogueWriter::~DaosCatalogueWriter() {

    clean();
    close();
}

bool DaosCatalogueWriter::createIndex(const Key& idxKey, size_t /* datumKeySize */) {
    return true;
}

bool DaosCatalogueWriter::selectIndex(const Key& idxKey) {

    currentIndexKey_ = idxKey;

    if (indexes_.find(idxKey) == indexes_.end()) {

        fdb5::DaosKeyValueName catalogue_kv{pool_, db_cont_, catalogue_kv_};

        fdb5::DaosSession s{};

        /// @note: performed RPCs:
        /// - generate catalogue kv oid (daos_obj_generate_oid)
        /// - ensure catalogue kv exists (daos_kv_open)
        fdb5::DaosKeyValue catalogue_kv_obj{s, catalogue_kv};

        int idx_loc_max_len = 512;  /// @todo: take from config

        try {

            std::vector<char> n((long)idx_loc_max_len);
            long res;

            /// @note: performed RPCs:
            /// - get index location from catalogue kv (daos_kv_get)
            res = catalogue_kv_obj.get(idxKey.valuesToString(), &n[0], idx_loc_max_len);

            indexes_[idxKey] = Index(new fdb5::DaosIndex(
                idxKey, *this, fdb5::DaosKeyValueName{eckit::URI{std::string{n.begin(), std::next(n.begin(), res)}}},
                false));
        }
        catch (fdb5::DaosEntityNotFoundException& e) {

            firstIndexWrite_ = true;

            indexes_[idxKey] = Index(new fdb5::DaosIndex(idxKey, *this, fdb5::DaosName{pool_, db_cont_}));

            /// index index kv in catalogue kv
            std::string nstr{indexes_[idxKey].location().uri().asString()};
            if (nstr.length() > idx_loc_max_len) {
                throw eckit::Exception("Serialised index location exceeded configured maximum index location length.");
            }
            /// @note: performed RPCs (only if the index wasn't visited yet and index kv doesn't exist yet, i.e. only on
            /// first write to an index key):
            /// - record index kv location into catalogue kv (daos_kv_put) -- always performed
            catalogue_kv_obj.put(idxKey.valuesToString(), nstr.data(), nstr.length());

            /// @note: performed RPCs:
            /// - close index kv when destroyed (daos_obj_close)
        }

        /// @note: performed RPCs:
        /// - close catalogue kv (daos_obj_close)
    }

    current_ = indexes_[idxKey];

    return true;
}

void DaosCatalogueWriter::deselectIndex() {

    current_ = Index();
    currentIndexKey_ = Key();
    firstIndexWrite_ = false;
}

void DaosCatalogueWriter::clean() {

    flush(0);

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
void DaosCatalogueWriter::archive(const Key& idxKey, const Key& datumKey,
                                  std::shared_ptr<const FieldLocation> fieldLocation) {

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    /// @note: the current index timestamp is undefined at this point
    Field field(std::move(fieldLocation), currentIndex().timestamp());

    /// @todo: is sorting axes really necessary?
    /// @note: sort in-memory axis values. Not triggering retrieval from DAOS axes.
    const_cast<fdb5::IndexAxis&>(current_.axes()).sort();

    /// before in-memory axes are updated as part of current_.put, we determine which
    /// additions will need to be performed on axes in DAOS after the field gets indexed.
    std::vector<std::string> axesToExpand;
    std::vector<std::string> valuesToAdd;
    std::string axisNames = "";
    std::string sep = "";

    for (Key::const_iterator i = datumKey.begin(); i != datumKey.end(); ++i) {

        const std::string& keyword = i->first;
        const std::string& value = i->second;

        if (value.length() == 0) {
            continue;
        }

        axisNames += sep + keyword;
        sep = ",";

        /// @note: obtain in-memory axis values. Not triggering retrieval from DAOS axes.
        /// @note: on first archive the in-memory axes will be empty and values() will return
        ///   empty sets. This is fine.
        const auto& axis_set = current_.axes().values(keyword);

        // if (!axis_set.has_value() || !axis_set->get().contains(value)) {
        if (!axis_set.contains(value)) {

            axesToExpand.push_back(keyword);
            valuesToAdd.push_back(value);
        }
    }

    /// index the field and update in-memory axes
    current_.put(datumKey, field);

    fdb5::DaosSession s{};

    /// persist axis names
    if (firstIndexWrite_) {

        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid{currentIndexKey_.valuesToString(), OC_S1};
        fdb5::DaosKeyValueName n{pool_, db_cont_, oid};

        /// @note: performed RPCs:
        /// - generate index kv oid (daos_obj_generate_oid)
        /// - ensure index kv exists (daos_obj_open)
        fdb5::DaosKeyValue kv{s, n};

        int axis_names_max_len = 512;
        if (axisNames.length() > axis_names_max_len) {
            throw eckit::Exception("Serialised axis names exceeded configured maximum axis names length.");
        }

        /// @note: performed RPCs:
        /// - record axis names into index kv (daos_kv_put)
        /// - close index kv when destroyed (daos_obj_close)
        kv.put("axes", axisNames.data(), axisNames.length());

        firstIndexWrite_ = false;
    }

    /// @todo: axes are supposed to be sorted before persisting. How do we do this with the DAOS approach?
    ///        sort axes every time they are loaded in the read pathway?

    if (axesToExpand.empty()) {
        return;
    }

    /// expand axis info in DAOS
    while (!axesToExpand.empty()) {

        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid{currentIndexKey_.valuesToString() + std::string{"."} + axesToExpand.back(), OC_S1};
        fdb5::DaosKeyValueName n{pool_, db_cont_, oid};

        /// @note: performed RPCs:
        /// - generate axis kv oid (daos_obj_generate_oid)
        /// - ensure axis kv exists (daos_obj_open)
        fdb5::DaosKeyValue kv{s, n};

        std::string v{"1"};

        /// @note: performed RPCs:
        /// - record axis value into axis kv (daos_kv_put)
        /// - close axis kv when destroyed (daos_obj_close)
        kv.put(valuesToAdd.back(), v.data(), v.length());

        axesToExpand.pop_back();
        valuesToAdd.pop_back();
    }
}

void DaosCatalogueWriter::flush(size_t archivedFields) {

    if (!current_.null()) {
        current_ = Index();
    }
}

void DaosCatalogueWriter::closeIndexes() {

    indexes_.clear();  // all indexes instances destroyed
}

static fdb5::CatalogueWriterBuilder<fdb5::DaosCatalogueWriter> builder("daos");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
