/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/fam/FamIndex.h"

#include "eckit/log/BigNum.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamFieldLocation.h"

// #include "fdb5/fam/FieldRef.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FamIndex::AutoCloser {
    FamIndex& index_;

public:
    explicit AutoCloser(FamIndex& index): index_(index) { }

    ~AutoCloser() { index_.close(); }

    AutoCloser(const AutoCloser&)                      = delete;
    AutoCloser& operator=(const AutoCloser&)           = delete;
    AutoCloser(AutoCloser&& other) noexcept            = delete;
    AutoCloser& operator=(AutoCloser&& other) noexcept = delete;
};

//----------------------------------------------------------------------------------------------------------------------

/// @note We use a FileStoreWrapper base that only exists to initialise the files_ member function
///       before the Index constructor is called. This is necessary as due to (preexisting)
///       serialisation ordering, the files_ member needs to be initialised from a Stream
///       before the type_ members of Index, but Indexs WILL be constructed before
///       the members of FamIndex

FamIndex::FamIndex(const Key& key, const eckit::FamRegionName& region, const Mode mode):
    IndexBase(key, FamCommon::typeName), mode_ {mode}, location_ {region.object(FamCommon::toString(key))} { }

FamIndex::~FamIndex() {
    close();
}

void FamIndex::encode(eckit::Stream& stream, const int version) const {
    // files_.encode(stream);
    IndexBase::encode(stream, version);
}

bool FamIndex::get(const Key& key, const Key& remapKey, Field& field) const {
    NOTIMP;

    // ASSERT(btree_);
    // FieldRef ref;
    // bool found = btree_->get(key.valuesToString(), ref);
    // if (found) {
    //     const eckit::URI& uri = files_.get(ref.uriId());
    //     FieldLocation*    loc =
    //         FieldLocationFactory::instance().build(uri.scheme(), uri, ref.offset(), ref.length(), remapKey);
    //     field = Field(std::move(*loc), timestamp_, ref.details());
    //     delete (loc);
    // }
    // return found;
}

void FamIndex::open() {
    LOG_DEBUG_LIB(LibFdb5) << "Opening " << *this << '\n';

    // if (!btree_) {
    //     LOG_DEBUG_LIB(LibFdb5) << "Opening " << *this << std::endl;
    //     btree_.reset(BTreeIndexFactory::build(type_, location_.path_, mode_ == FamIndex::READ, location_.offset_));
    //     if (mode_ == FamIndex::READ && preloadBTree_) { btree_->preload(); }
    // }
}

void FamIndex::reopen() {
    close();

    // Create a new btree at the end of this one
    // location_.offset_ = location_.path_.size();

    // The axes object must be reset at this point, as the FamIndex object is no longer referring
    // to the same region in memory. (i.e. the index is still associated with the same metadata
    // at the second level of the schema, but is a NEW index).

    axes_.wipe();

    open();
}

void FamIndex::close() {
    LOG_DEBUG_LIB(LibFdb5) << "Closing " << *this << '\n';
    // if (btree_) {
    //     LOG_DEBUG_LIB(LibFdb5) << "Closing " << *this << std::endl;
    //     btree_.reset();
    // }
}

void FamIndex::add(const Key& key, const Field& field) {
    NOTIMP;
    // ASSERT(btree_);
    // ASSERT(mode_ == FamIndex::WRITE);
    // FieldRef ref(files_, field);
    // //  bool replace =
    // btree_->set(key.valuesToString(), ref);  // returns true if replace, false if new insert
    // dirty_ = true;
}

void FamIndex::flush() {
    NOTIMP;
    // ASSERT(mode_ == FamIndex::WRITE);
    // if (dirty_) {
    //     axes_.sort();
    //     ASSERT(btree_);
    //     btree_->flush();
    //     btree_->sync();
    //     takeTimestamp();
    //     dirty_ = false;
    // }
}

void FamIndex::visit(IndexLocationVisitor& visitor) const {
    visitor(location_);
}

void FamIndex::flock() const {
    NOTIMP;
}

void FamIndex::funlock() const {
    NOTIMP;
}

// class FamIndexVisitor: public BTreeIndexVisitor {
//     const UriStore& files_;
//     EntryVisitor&   visitor_;
// public:
//     FamIndexVisitor(const UriStore& files, EntryVisitor& visitor): files_(files), visitor_(visitor) { }
//     void visit(const std::string& keyFingerprint, const FieldRef& ref) {
//         Field field(FamFieldLocation(files_, ref), visitor_.indexTimestamp(), ref.details());
//         visitor_.visitDatum(field, keyFingerprint);
//     }
// };

void FamIndex::entries(EntryVisitor& visitor) const {
    NOTIMP;

    // Index instantIndex(const_cast<FamIndex*>(this));
    // // Allow the visitor to selectively decline to visit the entries in this index
    // if (visitor.visitIndex(instantIndex)) {
    //     AutoCloser      closer(*this);
    //     FamIndexVisitor v(files_, visitor);
    //     btree_->visit(v);
    // }
}

void FamIndex::print(std::ostream& out) const {
    out << "FamIndex[location=" << location_ << ']';
}

auto FamIndex::dataURIs() const -> const std::vector<eckit::URI> {
    NOTIMP;
    // return files_.paths();
}

// class DumpBTreeVisitor: public BTreeIndexVisitor {
//     std::ostream& out_;
//     std::string   indent_;
// public:
//     DumpBTreeVisitor(std::ostream& out, const std::string& indent): out_(out), indent_(indent) { }
//     virtual ~DumpBTreeVisitor() { }
//     void visit(const std::string& key, const FieldRef& ref) {
//         out_ << indent_ << "Fingerprint: " << key << ", location: " << ref << std::endl;
//     }
// };

void FamIndex::dump(std::ostream& out, const char* indent, const bool simple, const bool dumpFields) const {
    NOTIMP;

    // out << indent << "Key: " << key_;
    // if (!simple) {
    //     out << std::endl;
    //     files_.dump(out, indent);
    //     axes_.dump(out, indent);
    // }
    //
    // if (dumpFields) {
    //     DumpBTreeVisitor v(out, std::string(indent) + std::string("  "));
    //
    //     out << std::endl;
    //     out << indent << "Contents of index: " << std::endl;
    //
    //     AutoCloser closer(*this);
    //     btree_->visit(v);
    // }
}

IndexStats FamIndex::statistics() const {
    NOTIMP;

    // IndexStats s(new FamIndexStats());
    // return s;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
