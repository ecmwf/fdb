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

#ifndef fdb5_TocIndex_H
#define fdb5_TocIndex_H

#include "eckit/eckit.h"

#include "eckit/container/BTree.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/memory/ScopedPtr.h"

#include "eckit/types/FixedString.h"

#include "fdb5/database/Index.h"

namespace fdb5 {

class BTreeIndex;

class TocIndex : public Index {

public: // types


public: // methods

    TocIndex(const Key &key, const eckit::PathName &path, off_t offset, Index::Mode mode, const std::string& type = defaulType());
    TocIndex(eckit::Stream &, const eckit::PathName &directory, const eckit::PathName &path, off_t offset);

    virtual ~TocIndex();

    static std::string defaulType();

private: // methods

    virtual void open();
    virtual void close();
    virtual void reopen();

    virtual bool get( const Key &key, Field &field ) const;
    virtual void add( const Key &key, const Field &field );
    virtual void flush();
    virtual void entries(EntryVisitor &visitor) const;

    virtual void print( std::ostream &out ) const;


private: // members

    eckit::ScopedPtr<BTreeIndex>  btree_;

    bool dirty_;

    friend class TocIndexCloser;

};



} // namespace fdb5

#endif
