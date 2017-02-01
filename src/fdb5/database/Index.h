/*
 * (C) Copyright 1996-2013 ECMWF.
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

#include "fdb5/database/Field.h"
#include "fdb5/database/FileStore.h"
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

class Index : private eckit::NonCopyable {

  public: // methods

    Index(const Key& key, const std::string& type);
    Index(eckit::Stream& s);

    virtual ~Index();

    virtual const IndexLocation& location() const = 0;

    virtual void open() = 0;
    virtual void reopen() = 0;
    virtual void close() = 0;
    virtual void flush() = 0;

    virtual void visit(IndexLocationVisitor& visitor) const = 0;

    const std::string& type() const;

    const IndexAxis& axes() const;
    const Key& key() const;

    virtual bool get(const Key &key, Field &field) const = 0;
    virtual void put(const Key &key, const Field &field);
    virtual void encode(eckit::Stream &s) const = 0;
    virtual void entries(EntryVisitor &visitor) const = 0;
    virtual void dump(std::ostream &out, const char* indent, bool simple = false) const = 0;

  private: // methods

    virtual void add(const Key &key, const Field &field) = 0;
    virtual void print( std::ostream &out ) const = 0;

  protected: // members

    std::string type_;

    /// @note Order of members is important here ...
    IndexAxis     axes_;   ///< This Index spans along these axis
    const Key     key_;    ///< key that selected this index
    std::string   prefix_;

    Indexer indexer_;

    friend std::ostream &operator<<(std::ostream &s, const Index &x) {
        x.print(s);
        return s;
    }

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
