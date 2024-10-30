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

/// @file   FamIndex.h
/// @author Metin Cakircali
/// @date   Jul 2024

#pragma once

#include "eckit/container/BTree.h"
#include "eckit/io/fam/FamRegionName.h"
#include "fdb5/database/Index.h"
#include "fdb5/fam/FamIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class BTreeIndex;

/// FileStoreWrapper exists _only_ so that the files_ member can be initialised from the stream
/// before the Index base class is initialised, for the FamIndex class. This order is required
/// to preserve the order that data is stored/read from streams from before the files_ object
/// was moved into the FamIndex class.
//
// struct UriStoreWrapper {
//     UriStoreWrapper(const eckit::PathName& directory): files_(directory) { }
//
//     UriStoreWrapper(const eckit::PathName& directory, eckit::Stream& s): files_(directory, s) { }
//
//     UriStore files_;
// };

//----------------------------------------------------------------------------------------------------------------------

class FamIndex: public IndexBase {
public:  // types
    // struct Sort;
    // enum class Type : char { CLOSED, WRITE, READ };

    enum class Mode : unsigned char { CLOSED = '\0', READ = 'r', WRITE = 'w' };

public:  // methods
    FamIndex(const Key& key, const eckit::FamRegionName& region, Mode mode);

    ~FamIndex() override;

    // static std::string defaulType();
    // auto path() const -> eckit::PathName { return location_.uri().path(); }
    // auto offset() const -> off_t { return location_.offset(); }

    void flock() const override;

    void funlock() const override;

private:  // methods
    class AutoCloser;

    friend AutoCloser;

    auto location() const -> const IndexLocation& override { return location_; }

    auto dataURIs() const -> const std::vector<eckit::URI> override;

    auto dirty() const -> bool override { return false; }

    void open() override;

    void close() override;

    void reopen() override;

    void visit(IndexLocationVisitor& visitor) const override;

    bool get(const Key& key, const Key& remapKey, Field& field) const override;
    void add(const Key& key, const Field& field) override;
    void flush() override;
    void encode(eckit::Stream& s, const int version) const override;
    void entries(EntryVisitor& visitor) const override;

    void print(std::ostream& out) const override;

    void dump(std::ostream& out, const char* indent, bool simple = false, bool dumpFields = false) const override;

    IndexStats statistics() const override;

private:  // members
    // bool dirty_ {false};

    Mode mode_ {Mode::CLOSED};

    FamIndexLocation location_;

    // std::unique_ptr<BTreeIndex> btree_;

    // In read-only mode, optimise (e.g. for pgen) by greedily reading entire btree
    // bool preloadBTree_;
};

//----------------------------------------------------------------------------------------------------------------------

// /// Useful for specifying order within the FamHandler/FamDBReader
// /// It can be helpful to iterate through indexes sequentially according to how they are on disk (i.e. all the indexes
// /// in one file sequentially in the order written).
//
// struct FamIndex::Sort {
//     // Return true if first argument is earlier than the second, and false otherwise.
//     bool operator()(const Index& lhs, const Index& rhs) {
//         const auto* idx1 = dynamic_cast<const FamIndex*>(lhs.content());
//         const auto* idx2 = dynamic_cast<const FamIndex*>(rhs.content());
//
//         ASSERT(idx1 != nullptr);
//         ASSERT(idx2 != nullptr);
//
//         const eckit::PathName& pth1(idx1->path());
//         const eckit::PathName& pth2(idx2->path());
//         if (pth1 == pth2) { return idx1->offset() < idx2->offset(); }
//
//         return pth1 < pth2;
//     }
// };

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
