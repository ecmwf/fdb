/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "eckit/log/BigNum.h"

// #include "fdb5/LibFdb5.h"
// #include "fdb5/toc/TocStats.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/HandleStream.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosIndex.h"
// #include "fdb5/toc/BTreeIndex.h"
// #include "fdb5/toc/FieldRef.h"
// #include "fdb5/toc/TocFieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// class TocIndexCloser {

//     const TocIndex &index_;
//     bool opened_;

// public:
//     TocIndexCloser(const TocIndex &index): index_(index), opened_(index.btree_) {
//         if (!opened_) {
//             const_cast<TocIndex&>(index_).open();
//         }
//     }
//     ~TocIndexCloser() {
//         if (!opened_) {
//             const_cast<TocIndex&>(index_).close();
//         }
//     }
// };

//----------------------------------------------------------------------------------------------------------------------

// /// @note We use a FileStoreWrapper base that only exists to initialise the files_ member function
// ///       before the Index constructor is called. This is necessary as due to (preexisting)
// ///       serialisation ordering, the files_ member needs to be initialised from a Stream
// ///       before the type_ members of Index, but Indexs WILL be constructed before
// ///       the members of TocIndex

DaosIndex::DaosIndex(const Key& key, const fdb5::DaosKeyValueName& name) :
    // UriStoreWrapper(path.dirName()),
    IndexBase(key, "daosKeyValue"),
    // btree_(nullptr),
    // dirty_(false),
    // mode_(mode),
    location_(name, 0) {
}

// TocIndex::TocIndex(eckit::Stream &s, const int version, const eckit::PathName &directory, const eckit::PathName &path, off_t offset):
//     UriStoreWrapper(directory, s),
//     IndexBase(s, version),
//     btree_(nullptr),
//     dirty_(false),
//     mode_(TocIndex::READ),
//     location_(path, offset) {
// }

// TocIndex::~TocIndex() {
//     close();
// }

bool DaosIndex::mayContain(const Key &key) const {

    /// @todo: in-memory axes_ are left empty in the read pathway. Alternatively, could
    ///        consider reading axes content from DAOS in one go upon DaosIndex creation,
    ///        and have mayContain check these in-memory axes. However some mechanism
    ///        would have to be implemented for readers to lock DAOS KVs storing axis 
    ///        information so that in-memory axes are consistent with DAOS axes.

    std::string indexKey{key_.valuesToString()};

    for (Key::const_iterator i = key.begin(); i != key.end(); ++i) {

        const std::string &keyword = i->first;
        std::string value = key.canonicalValue(keyword);

        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid{indexKey + std::string{"."} + keyword, OC_S1};
        fdb5::DaosKeyValueName axis{location_.daosName().poolName(), location_.daosName().contName(), oid};

        if (!axis.exists()) return false;
        if (!axis.has(value)) return false;

    }

    return true;

}

// void TocIndex::encode(eckit::Stream& s, const int version) const {
//     files_.encode(s);
//     IndexBase::encode(s, version);
// }


bool DaosIndex::get(const Key &key, const Key &remapKey, Field &field) const {

    const fdb5::DaosKeyValueName& n = location_.daosName();
    ASSERT(n.exists());

    std::string query{key.valuesToString()};

    if (!n.has(query)) return false;

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue index{s, n};
    std::vector<char> loc_data((long) index.size(query));
    index.get(query, &loc_data[0], loc_data.size());
    eckit::MemoryStream ms{&loc_data[0], loc_data.size()};
    field = fdb5::Field(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms), timestamp_, fdb5::FieldDetails());

    return true;

    // ASSERT(btree_);
    // FieldRef ref;

    // bool found = btree_->get(key.valuesToString(), ref);
    // if ( found ) {
    //     const eckit::URI& uri = files_.get(ref.uriId());
    //     field = Field(FieldLocationFactory::instance().build(uri.scheme(), uri, ref.offset(), ref.length(), remapKey), timestamp_, ref.details());
    // }
    // return found;

}


// void DaosIndex::open() {

//     if (!btree_) {
//         eckit::Log::debug<LibFdb5>() << "Opening " << *this << std::endl;
//         btree_.reset(BTreeIndexFactory::build(type_, location_.path_, mode_ == TocIndex::READ, location_.offset_));
//     }

// }

// void TocIndex::reopen() {
//     close();

//     // Create a new btree at the end of this one

//     location_.offset_ = location_.path_.size();

//     // The axes object must be reset at this point, as the TocIndex object is no longer referring
//     // to the same region in memory. (i.e. the index is still associated with the same metadata
//     // at the second level of the schema, but is a NEW index).

//     axes_.wipe();

//     open();
// }

// void TocIndex::close() {
//     if (btree_) {
//         eckit::Log::debug<LibFdb5>() << "Closing " << *this << std::endl;
//         btree_.reset();
//     }
// }

void DaosIndex::add(const Key &key, const Field &field) {

    // ASSERT(mode_ == TocIndex::WRITE);

    // FieldRef ref(files_, field);

    eckit::MemoryHandle h{(size_t) PATH_MAX};
    eckit::HandleStream hs{h};
    h.openForWrite(eckit::Length(0));
    {
        eckit::AutoClose closer(h);
        hs << field.location();
    }
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue{s, location_.daosName()}.put(key.valuesToString(), h.data(), hs.bytesWritten());   

    // //  bool replace =
    // btree_->set(key.valuesToString(), ref); // returns true if replace, false if new insert

    // dirty_ = true;

}

// void TocIndex::flush() {
//     ASSERT( mode_ == TocIndex::WRITE );

//     if (dirty_) {
//         axes_.sort();
//         ASSERT(btree_);
//         btree_->flush();
//         btree_->sync();
//         takeTimestamp();
//         dirty_ = false;
//     }
// }


// void TocIndex::visit(IndexLocationVisitor &visitor) const {
//     visitor(location_);
// }

// void TocIndex::flock() const {
//     ASSERT(btree_);
//     btree_->flock();
// }
// void TocIndex::funlock() const {
//     ASSERT(btree_);
//     btree_->funlock();
// }

// class TocIndexVisitor : public BTreeIndexVisitor {
//     const UriStore &files_;
//     EntryVisitor &visitor_;
// public:
//     TocIndexVisitor(const UriStore &files, EntryVisitor &visitor):
//         files_(files),
//         visitor_(visitor) {}

//     void visit(const std::string& keyFingerprint, const FieldRef& ref) {
//         Field field(new TocFieldLocation(files_, ref), visitor_.indexTimestamp(), ref.details());
//         visitor_.visitDatum(field, keyFingerprint);
//     }
// };

// void TocIndex::entries(EntryVisitor &visitor) const {
//     TocIndexCloser closer(*this);

//     Index instantIndex(const_cast<TocIndex*>(this));

//     // Allow the visitor to selectively decline to visit the entries in this index
//     if (visitor.visitIndex(instantIndex)) {
//         TocIndexVisitor v(files_, visitor);
//         btree_->visit(v);
//     }
// }

// void TocIndex::print(std::ostream &out) const {
//     out << "TocIndex(path=" << location_.path_ << ",offset="<< location_.offset_ << ")";
// }


// std::string TocIndex::defaulType() {
//     return BTreeIndex::defaulType();
// }

// const std::vector<eckit::URI> TocIndex::dataPaths() const {
//     return files_.paths();
// }

// bool TocIndex::dirty() const {
//     return dirty_;
// }


// class DumpBTreeVisitor : public BTreeIndexVisitor {
//     std::ostream& out_;
//     std::string indent_;
// public:

//     DumpBTreeVisitor(std::ostream& out, const std::string& indent) : out_(out), indent_(indent) {}
//     virtual ~DumpBTreeVisitor() {}

//     void visit(const std::string& key, const FieldRef& ref) {

//         out_ << indent_ << "Fingerprint: " << key << ", location: " << ref << std::endl;
//     }
// };


// void TocIndex::dump(std::ostream &out, const char* indent, bool simple, bool dumpFields) const {
//     out << indent << "Key: " << key_;

//     if(!simple) {
//         out << std::endl;
//         files_.dump(out, indent);
//         axes_.dump(out, indent);
//     }

//     if (dumpFields) {
//         DumpBTreeVisitor v(out, std::string(indent) + std::string("  "));

//         out << std::endl;
//         out << indent << "Contents of index: " << std::endl;

//         TocIndexCloser closer(*this);
//         btree_->visit(v);
//     }
// }

// IndexStats TocIndex::statistics() const
// {
//     IndexStats s(new TocIndexStats());
//     return s;
// }

//-----------------------------------------------------------------------------

} // namespace fdb5
