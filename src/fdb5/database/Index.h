/*
 * (C) Copyright 1996-2017 ECMWF.
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

#ifndef fdb5_Index_H
#define fdb5_Index_H

#include "eckit/eckit.h"

#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/types/FixedString.h"
#include "eckit/types/Types.h"
#include "eckit/memory/Counted.h"

#include "fdb5/database/Field.h"
#include "fdb5/database/FileStore.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/IndexLocation.h"
#include "fdb5/database/Indexer.h"
#include "fdb5/database/Key.h"

namespace eckit {
class Stream;
}

namespace fdb5 {

class Key;
class Index;
class IndexLocationVisitor;
class Schema;

//----------------------------------------------------------------------------------------------------------------------


class EntryVisitor : private eckit::NonCopyable {
public:

    virtual ~EntryVisitor();

    virtual void visit(const Index& index,
                       const Field& field,
                       const std::string& indexFingerprint,
                       const std::string& fieldFingerprint) = 0;
};


//----------------------------------------------------------------------------------------------------------------------


class DumpVisitor : public fdb5::EntryVisitor {

public:
    DumpVisitor(std::ostream& out, Schema& schema, Key& dbKey) :
        out_(out), schema_(schema), dbKey_(dbKey) {}

private:

    virtual void visit(const Index& index,
                       const Field& field,
                       const std::string& indexFingerprint,
                       const std::string& fieldFingerprint);

private: // members
    std::ostream& out_;
    Schema& schema_;
    Key& dbKey_;
};

//----------------------------------------------------------------------------------------------------------------------

/// Base class for Indexing fields in the FDB
///
/// Each concrete type of DB can implement a specific type of Index

class IndexBase : public eckit::Counted {

public: // methods

    IndexBase(const Key& key, const std::string& type);
    IndexBase(eckit::Stream& s);

    virtual ~IndexBase();

    virtual const IndexLocation& location() const = 0;

    virtual bool dirty() const = 0;

    virtual void open() = 0;
    virtual void reopen() = 0;
    virtual void close() = 0;

    /// Flush and Sync data (for mediums where sync() is required)
    virtual void flush() = 0;

    virtual void visit(IndexLocationVisitor& visitor) const = 0;

    const std::string& type() const;

    const IndexAxis& axes() const;
    const Key& key() const;

    virtual bool get(const Key &key, Field &field) const = 0;
    virtual void put(const Key &key, const Field &field);

    virtual void encode(eckit::Stream &s) const = 0;
    virtual void entries(EntryVisitor &visitor) const = 0;
    virtual void dump(std::ostream &out, const char* indent, bool simple = false, bool dumpFields = false) const = 0;

    virtual bool mayContain(const Key& key) const;

    virtual IndexStats statistics() const = 0;

    virtual void print( std::ostream &out ) const = 0;

private: // methods

    virtual void add(const Key &key, const Field &field) = 0;

protected: // members

    std::string type_;

    /// @note Order of members is important here ...
    IndexAxis     axes_;   ///< This Index spans along these axis
    const Key     key_;    ///< key that selected this index
    std::string   prefix_;

    Indexer indexer_;

    friend std::ostream& operator<<(std::ostream& s, const IndexBase& o) {
        o.print(s); return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

class Index {

public: // methods

    Index();
    Index(IndexBase* i);

    ~Index();

    Index(const Index&);
    Index& operator=(const Index&);

    const IndexLocation& location() const { return content_->location(); }

    bool dirty() const { return content_->dirty(); }

    void open()   { return content_->open();   }
    void reopen() { return content_->reopen(); }
    void close()  { return content_->close();  }
    void flush()  { return content_->flush();  }

    void visit(IndexLocationVisitor& visitor) const { content_->visit(visitor); }

    const std::string& type() const { return content_->type(); }

    const IndexAxis& axes() const { return content_->axes(); }
    const Key& key() const { return content_->key(); }

    bool get(const Key& key, Field& field) const { return content_->get(key, field); }
    void put(const Key& key, const Field& field) { content_->put(key, field); }

    void encode(eckit::Stream& s) const { content_->encode(s); }
    void entries(EntryVisitor& v) const { content_->entries(v); }
    void dump(std::ostream &out, const char* indent, bool simple = false, bool dumpFields = false) const {
        content_->dump(out, indent, simple, dumpFields);
    }

    IndexStats statistics() const { return content_->statistics(); }

    IndexBase* content() { return content_; }
    const IndexBase* content() const { return content_; }

    bool mayContain(const Key& key) const { return content_->mayContain(key); }

    bool null() const { return null_; }

    friend bool operator<  (const Index& i1, const Index& i2) { return i1.content_ <  i2.content_; }
    friend bool operator== (const Index& i1, const Index& i2) { return i1.content_ == i2.content_; }

private: // methods

    void print(std::ostream& s) const { content_->print(s); }

    friend std::ostream& operator<<(std::ostream& s, const Index& o) {
        o.print(s); return s;
    }

private: // members

    IndexBase* content_;
    bool null_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
