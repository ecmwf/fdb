/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_TocIndex_H
#define fdb5_TocIndex_H

#include "eckit/eckit.h"

#include "eckit/container/BTree.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"

#include "eckit/types/FixedString.h"

#include "fdb5/database/Index.h"
#include "fdb5/database/UriStore.h"
#include "fdb5/toc/TocIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class BTreeIndex;


/// FileStoreWrapper exists _only_ so that the uris_ member can be initialised from the stream
/// before the Index base class is initialised, for the TocIndex class. This order is required
/// to preserve the order that data is stored/read from streams from before the uris_ object
/// was moved into the TocIndex class.

struct UriStoreWrapper {

    UriStoreWrapper(const eckit::PathName& directory) : uris_(directory) {}
    UriStoreWrapper(const eckit::PathName& directory, eckit::Stream& s) : uris_(directory, s) {}

    UriStore uris_;
};

//----------------------------------------------------------------------------------------------------------------------


class TocIndex : private UriStoreWrapper, public IndexBase {

public:  // types

    enum Mode {
        WRITE,
        READ
    };

public:  // methods

    TocIndex(const Key& key, const eckit::PathName& path, off_t offset, Mode mode,
             const std::string& type = defaulType());

    TocIndex(eckit::Stream&, const int version, const eckit::PathName& directory, const eckit::PathName& path,
             off_t offset, bool preloadBTree = false);

    ~TocIndex() override;

    static std::string defaulType();

    eckit::PathName path() const { return location_.uri().path(); }
    off_t offset() const { return location_.offset(); }

    void flock() const override;
    void funlock() const override;

private:  // methods

    const IndexLocation& location() const override { return location_; }

    std::vector<eckit::URI> dataURIs() const override;

    bool dirty() const override;

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

    std::unique_ptr<BTreeIndex> btree_;

    bool dirty_;

    friend class TocIndexCloser;

    const TocIndex::Mode mode_;

    TocIndexLocation location_;

    // In read-only mode, optimise (e.g. for pgen) by greedily reading entire btree
    bool preloadBTree_;
};

//----------------------------------------------------------------------------------------------------------------------

/// Useful for specifying order within the TocHandler/TocDBReader
/// It can be helpful to iterate through indexes sequentially according to how they are on disk (i.e. all the indexes
/// in one file sequentially in the order written).

struct TocIndexFileSort {

    // Return true if first argument is earlier than the second, and false otherwise.
    bool operator()(const Index& lhs, const Index& rhs) {

        const TocIndex* idx1 = dynamic_cast<const TocIndex*>(lhs.content());
        const TocIndex* idx2 = dynamic_cast<const TocIndex*>(rhs.content());

        ASSERT(idx1);
        ASSERT(idx2);

        const eckit::PathName& pth1(idx1->path());
        const eckit::PathName& pth2(idx2->path());

        if (pth1 == pth2) {
            return idx1->offset() < idx2->offset();
        }
        else {
            return pth1 < pth2;
        }
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
