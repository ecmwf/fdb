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

#ifndef fdb5_Index_H
#define fdb5_Index_H

#include <ctime>
#include <iosfwd>
#include <vector>

#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/Counted.h"
#include "eckit/types/Types.h"

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/IndexLocation.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/Indexer.h"


namespace eckit {
class Stream;
}

namespace fdb5 {

class Index;
class IndexLocationVisitor;
class Rule;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

/// Base class for Indexing fields in the FDB
///
/// Each concrete type of DB can implement a specific type of Index

class IndexBase : public eckit::Counted {

public:  // methods

    IndexBase(const Key& key, const std::string& type);
    IndexBase(eckit::Stream& s, const int version);

    ~IndexBase() override;

    virtual const IndexLocation& location() const = 0;

    virtual std::vector<eckit::URI> dataURIs() const { NOTIMP; }

    virtual bool dirty() const = 0;

    virtual void open()   = 0;
    virtual void reopen() = 0;
    virtual void close()  = 0;

    /// Flush and Sync data (for mediums where sync() is required)
    virtual void flush() = 0;

    virtual void visit(IndexLocationVisitor& visitor) const = 0;

    const std::string& type() const;

    const IndexAxis& axes() const;
    const Key& key() const;

    time_t timestamp() const { return timestamp_; }

    virtual bool get(const Key& key, const Key& remapKey, Field& field) const = 0;
    virtual void put(const Key& key, const Field& field);

    virtual void encode(eckit::Stream& s, const int version) const;
    virtual void entries(EntryVisitor& visitor) const = 0;

    /// @note default args on virtual methods is not best practice; no guarantee that overrides will have same defaults
    virtual void dump(std::ostream& out, const char* indent, bool simple = false, bool dumpFields = false) const = 0;

    bool partialMatch(const metkit::mars::MarsRequest& indexRequest,
                      const metkit::mars::MarsRequest& datumRequest) const;
    virtual bool mayContain(const Key& key) const;
    virtual bool mayContainPartial(const Key& key) const;

    virtual IndexStats statistics() const = 0;

    virtual void print(std::ostream& out) const = 0;

    virtual void flock() const   = 0;
    virtual void funlock() const = 0;

protected:  // methods

    void takeTimestamp() { time(&timestamp_); }

private:  // methods

    void encodeCurrent(eckit::Stream& s, int version) const;
    void encodeLegacy(eckit::Stream& s, int version) const;

    void decodeCurrent(eckit::Stream& s, int version);
    void decodeLegacy(eckit::Stream& s, int version);

    virtual void add(const Key& key, const Field& field) = 0;

protected:  // members

    std::string type_;

    /// @note Order of members is important here ...
    IndexAxis axes_;       ///< This Index spans along these axis
    Key key_;              ///< key that selected this index
    time_t timestamp_{0};  ///< timestamp when this Index was flushed

    Indexer indexer_;

    friend std::ostream& operator<<(std::ostream& s, const IndexBase& o) {
        o.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

class Index {

public:  // methods

    Index();
    Index(IndexBase* i);

    ~Index();

    Index(const Index&);
    Index& operator=(const Index&);

    const IndexLocation& location() const { return content_->location(); }

    std::vector<eckit::URI> dataURIs() const { return content_->dataURIs(); }

    bool dirty() const { return content_->dirty(); }

    void open() { content_->open(); }

    void reopen() { content_->reopen(); }

    void close() { content_->close(); }

    void flush() { content_->flush(); }

    void visit(IndexLocationVisitor& visitor) const { content_->visit(visitor); }

    const std::string& type() const { return content_->type(); }

    const IndexAxis& axes() const { return content_->axes(); }
    const Key& key() const { return content_->key(); }

    time_t timestamp() const { return content_->timestamp(); }

    bool get(const Key& key, const Key& remapKey, Field& field) const { return content_->get(key, remapKey, field); }
    void put(const Key& key, const Field& field) { content_->put(key, field); }

    void encode(eckit::Stream& s, const int version) const { content_->encode(s, version); }
    void entries(EntryVisitor& v) const { content_->entries(v); }
    void dump(std::ostream& out, const char* indent, bool simple = false, bool dumpFields = false) const {
        content_->dump(out, indent, simple, dumpFields);
    }

    IndexStats statistics() const { return content_->statistics(); }

    IndexBase* content() { return content_; }
    const IndexBase* content() const { return content_; }

    bool partialMatch(const metkit::mars::MarsRequest& indexRequest,
                      const metkit::mars::MarsRequest& datumRequest) const {
        return content_->partialMatch(indexRequest, datumRequest);
    }
    bool mayContain(const Key& key) const { return content_->mayContain(key); }
    bool mayContainPartial(const Key& key) const { return content_->mayContainPartial(key); }

    bool null() const { return null_; }

    friend bool operator<(const Index& i1, const Index& i2) { return i1.content_ < i2.content_; }
    friend bool operator==(const Index& i1, const Index& i2) { return i1.content_ == i2.content_; }

    void flock() const { content_->flock(); }
    void funlock() const { content_->funlock(); }

private:  // methods

    void print(std::ostream& s) const { content_->print(s); }

    friend std::ostream& operator<<(std::ostream& s, const Index& o) {
        o.print(s);
        return s;
    }

private:  // members

    IndexBase* content_;
    bool null_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
