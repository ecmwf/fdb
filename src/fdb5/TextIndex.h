/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino

#ifndef fdb5_TextIndex_H
#define fdb5_TextIndex_H

#include "eckit/eckit.h"

#include "eckit/io/Length.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/io/Offset.h"

#include "fdb5/Index.h"
#include "fdb5/FileStore.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TextIndex : public Index {

public: // methods

    TextIndex(const Key& key, const eckit::PathName& path, Index::Mode mode);

    virtual ~TextIndex();

private: // methods

    virtual bool    exists( const Key& key ) const;

    virtual bool    get( const Key& key, Field& field ) const;

    virtual Field   get( const Key& key ) const;

    virtual void    put_( const Key& key, const Field& field );

    virtual bool    remove( const Key& key );

    virtual void flush();

    virtual void entries(EntryVisitor& visitor) const;

    void save(const eckit::PathName& path) const;
    void load(const eckit::PathName& path);

    virtual void list( std::ostream& out ) const;

    virtual void print( std::ostream& out ) const;

private: // types

    typedef FileStore::FieldRef FieldRef;

    typedef std::map< Key, FieldRef > FieldStore;

private: // members

    FieldStore  store_;

    bool flushed_;
    bool fdbCheckDoubleInsert_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
