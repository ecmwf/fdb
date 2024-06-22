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
// #include <numeric>

#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/serialisation/HandleStream.h"

#include "fdb5/LibFdb5.h"

// #include "fdb5/daos/DaosSession.h"
// #include "fdb5/daos/DaosName.h"
#include "eckit/io/rados/RadosException.h"
#include "eckit/io/rados/RadosKeyValue.h"

#include "fdb5/rados/RadosIndex.h"
#include "fdb5/rados/RadosCatalogueWriter.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosCatalogueWriter::RadosCatalogueWriter(const Key &key, const fdb5::Config& config) :
    RadosCatalogue(key, config), firstIndexWrite_(false) {

    /// @note: performed RPCs:
    /// - daos_pool_connect
    /// - root cont open (daos_cont_open)
    /// - root cont create (daos_cont_create)
#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    std::string db_name = db_namespace_;
    ASSERT(root_kv_->nspace().pool().exists());
#else
    std::string db_name = db_pool_;
    root_kv_->nspace().pool().ensureCreated();
#endif

    /// @note: the DaosKeyValue constructor checks if the kv exists, which results in creation if not exists
    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)

    /// @note: performed RPCs:
    /// - check if main kv contains db key (daos_kv_get without a buffer)
    if (!root_kv_->has(db_name)) {

        /// create catalogue kv
        db_kv_->ensureCreated();

        /// write schema under "schema"
        eckit::Log::debug<LibFdb5>() << "Copy schema from "
                                     << config_.schemaPath()
                                     << " to "
                                     << db_kv_->uri().asString()
                                     << " at key 'schema'."
                                     << std::endl;

        eckit::FileHandle in(config_.schemaPath());
        std::vector<char> data;
        data.resize(in.size());
        in.read(&data[0], in.size());
        db_kv_->put("schema", &data[0], data.size());

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
        
        db_kv_->put("key", h.data(), hs.bytesWritten());   

        /// index newly created catalogue kv in main kv
        int db_loc_max_len = 512;  // @todo: take from config
        std::string nstr = db_kv_->uri().asString();
        if (nstr.length() > db_loc_max_len) 
            throw eckit::Exception("Serialised db location exceeded configured maximum db location length.");

        root_kv_->put(db_name, nstr.data(), nstr.length());

    }
    
    /// @todo: record or read dbUID

    /// @note: performed RPCs:
    /// - catalogue container open (daos_cont_open)
    /// - get schema from catalogue kv (daos_kv_get)
    RadosCatalogue::loadSchema();

    /// @todo: TocCatalogue::checkUID();

}

RadosCatalogueWriter::RadosCatalogueWriter(const eckit::URI &uri, const fdb5::Config& config) :
    RadosCatalogue(uri, ControlIdentifiers{}, config), firstIndexWrite_(false) {

    NOTIMP;

}

RadosCatalogueWriter::~RadosCatalogueWriter() {

    clean();
    close();

}

bool RadosCatalogueWriter::selectIndex(const Key& key) {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    std::string pool = pool_;
    std::string nspace = db_namespace_;
#else
    std::string pool = db_pool_;
    std::string nspace = namespace_;
#endif

    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        /// @note: performed RPCs:
        /// - generate catalogue kv oid (daos_obj_generate_oid)
        /// - ensure catalogue kv exists (daos_kv_open)

        int idx_loc_max_len = 512;  /// @todo: take from config

        try {

            std::vector<char> n((long) idx_loc_max_len);
            long res;

            /// @note: performed RPCs:
            /// - get index location from catalogue kv (daos_kv_get)
            res = db_kv_->get(key.valuesToString(), &n[0], idx_loc_max_len);

            indexes_[key] = Index(
                new fdb5::RadosIndex(
                    key, 
// #ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE
//                     eckit::RadosPersistentKeyValue{eckit::URI{std::string{n.begin(), std::next(n.begin(), res)}}, true},
// #elif fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
//                     eckit::RadosPersistentKeyValue{eckit::URI{std::string{n.begin(), std::next(n.begin(), res)}}},
// #else
                    eckit::RadosKeyValue{eckit::URI{std::string{n.begin(), std::next(n.begin(), res)}}},
// #endif
                    false
                )
            );

        } catch (eckit::RadosEntityNotFoundException& e) {

            firstIndexWrite_ = true;
 
            indexes_[key] = Index(
                new fdb5::RadosIndex(
                    key, 
                    eckit::RadosNamespace{pool, nspace}
                )
            );

            /// index index kv in catalogue kv
            std::string nstr{indexes_[key].location().uri().asString()};
            if (nstr.length() > idx_loc_max_len)
                throw eckit::Exception("Serialised index location exceeded configured maximum index location length.");
            /// @note: performed RPCs (only if the index wasn't visited yet and index kv doesn't exist yet, i.e. only on first write to an index key):
            /// - record index kv location into catalogue kv (daos_kv_put) -- always performed
            db_kv_->put(key.valuesToString(), nstr.data(), nstr.length());

            /// @note: performed RPCs:
            /// - close index kv when destroyed (daos_obj_close)

        }

        /// @note: performed RPCs:
        /// - close catalogue kv (daos_obj_close)

    }

    current_ = indexes_[key];

    return true;

}

void RadosCatalogueWriter::deselectIndex() {

    current_ = Index();
    currentIndexKey_ = Key();
    firstIndexWrite_ = false;

}

void RadosCatalogueWriter::clean() {

    flush();

    deselectIndex();

}

void RadosCatalogueWriter::close() {

    closeIndexes();

}

const Index& RadosCatalogueWriter::currentIndex() {

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    return current_;

}

/// @todo: other writers may be simultaneously updating the axes KeyValues in DAOS. Should these
///        new updates be retrieved and put into in-memory axes from time to time, e.g. every
///        time a value is put in an axis KeyValue?
void RadosCatalogueWriter::archive(const Key& key, std::unique_ptr<FieldLocation> fieldLocation) {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    std::string pool = pool_;
    std::string nspace = db_namespace_;
#else
    std::string pool = db_pool_;
    std::string nspace = namespace_;
#endif

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

    for (Key::const_iterator i = key.begin(); i != key.end(); ++i) {

        const std::string &keyword = i->first;

        std::string value = key.canonicalValue(keyword);

        if (value.length() == 0) continue;

        axisNames += sep + keyword;
        sep = ",";

        /// @note: obtain in-memory axis values. Not triggering retrieval from DAOS axes.
        /// @note: on first archive the in-memory axes will be empty and values() will return
        ///   empty sets. This is fine.
        const auto& axis_set = current_.axes().values(keyword);

        //if (!axis_set.has_value() || !axis_set->get().contains(value)) {
        if (!axis_set.contains(value)) {

            axesToExpand.push_back(keyword);
            valuesToAdd.push_back(value);

        }

    }

    /// index the field and update in-memory axes
    current_.put(key, field);

    /// persist axis names
    if (firstIndexWrite_) {

        /// @note: performed RPCs:
        /// - generate index kv oid (daos_obj_generate_oid)
        /// - ensure index kv exists (daos_obj_open)

        int axis_names_max_len = 512;
        if (axisNames.length() > axis_names_max_len)
            throw eckit::Exception("Serialised axis names exceeded configured maximum axis names length.");

        /// @note: performed RPCs:
        /// - record axis names into index kv (daos_kv_put)
        /// - close index kv when destroyed (daos_obj_close)
        dynamic_cast<fdb5::RadosIndex*>(current_.content())->putAxisNames(axisNames);

        firstIndexWrite_ = false;

    }

    /// @todo: axes are supposed to be sorted before persisting. How do we do this with the DAOS approach?
    ///        sort axes every time they are loaded in the read pathway?

    if (axesToExpand.empty()) return;

    /// expand axis info in DAOS
    while (!axesToExpand.empty()) {

        /// @note: performed RPCs:
        /// - generate axis kv oid (daos_obj_generate_oid)
        /// - ensure axis kv exists (daos_obj_open)

        /// @note: performed RPCs:
        /// - record axis value into axis kv (daos_kv_put)
        /// - close axis kv when destroyed (daos_obj_close)
        dynamic_cast<fdb5::RadosIndex*>(current_.content())->putAxisValue(axesToExpand.back(), valuesToAdd.back());

        axesToExpand.pop_back();
        valuesToAdd.pop_back();

    }

}

void RadosCatalogueWriter::flush() {

#ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
    for (IndexStore::iterator j = indexes_.begin(); j != indexes_.end(); ++j )
        j->second.flush();
flush axis kvs if not done as part of RadosIndex::flush
    db_kv_->flush();
    root_kv_->flush();
#endif

    if (!current_.null()) current_ = Index();

}

void RadosCatalogueWriter::closeIndexes() {

    indexes_.clear(); // all indexes instances destroyed

}

static fdb5::CatalogueBuilder<fdb5::RadosCatalogueWriter> builder("rados.writer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
