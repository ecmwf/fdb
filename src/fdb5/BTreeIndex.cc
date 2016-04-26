/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"

#include "fdb5/Key.h"
#include "fdb5/BTreeIndex.h"
#include "fdb5/Error.h"

namespace fdb5 {

//-----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const Key& key, const eckit::PathName& path, Index::Mode mode ) :
    Index(key, path, mode),
    btree_( path, bool( mode == Index::READ ) ),
    fdbCheckDoubleInsert_( eckit::Resource<bool>("fdbCheckDoubleInsert", true) ),
    dirty_(false)
{
}

BTreeIndex::~BTreeIndex()
{
}

bool BTreeIndex::exists(const Key &key) const
{
    BTreeKey k (key.valuesToString());
    FieldRef ignore;
    return const_cast<BTreeIndex*>(this)->btree_.get(k,ignore);
}

bool BTreeIndex::get(const Key &key, Index::Field& field) const
{
    FieldRef ref;
    BTreeKey k (key.valuesToString());
    bool found = const_cast<BTreeIndex*>(this)->btree_.get(k,ref);
    if( found )
    {
		field.path_     = files_.get( ref.pathId_ );
        field.offset_   = ref.offset_;
        field.length_   = ref.length_;
    }
    return found;
}

BTreeIndex::Field BTreeIndex::get(const Key& key) const
{
    Field result;
    FieldRef ref;
    BTreeKey k (key.valuesToString());
    eckit::Log::info() << "BTreeIndex get " << key << " (" << k << ")" << std::endl;
    bool found = const_cast<BTreeIndex*>(this)->btree_.get(k,ref);
    if( !found ) {
           std::ostringstream oss;
           oss << "FDB key not found " << key;
           throw eckit::BadParameter(oss.str(), Here());
    }

	result.path_     = files_.get( ref.pathId_ );
    result.offset_   = ref.offset_;
    result.length_   = ref.length_;

    return result;
}

void BTreeIndex::put_(const Key& key, const BTreeIndex::Field& field)
{
    ASSERT( mode() == Index::WRITE );


    BTreeKey k( key.valuesToString() );
    FieldRef ref;

    eckit::Log::info() << "BTreeIndex insert " << key << " (" << k << ") = " << field << std::endl;

	ref.pathId_ = files_.insert( field.path_ ); // inserts not yet in filestore
    ref.offset_ = field.offset_;
    ref.length_ = field.length_;

    bool replace = btree_.set(k,ref);  // returns true if replace, false if new insert

    dirty_ = true;

    if(fdbCheckDoubleInsert_ && replace) {
        std::ostringstream oss;
        oss << "Duplicate FDB entry with key: " << key << " -- This may be a schema bug in the fdbRules";
        throw fdb5::Error(Here(), oss.str());
    }
}

bool BTreeIndex::remove(const Key& key)
{
    NOTIMP;

    ASSERT( mode() == Index::WRITE );

    dirty_ = true;

    BTreeKey k( key.valuesToString() );
	return btree_.remove(k);
}

void BTreeIndex::flush()
{
    ASSERT( mode() == Index::WRITE );

    if(dirty_) {
        btree_.flush();
        dirty_ = false;
    }

    Index::flush();
}

void BTreeIndex::print(std::ostream& out) const
{
    out << "BTreeIndex[]";
}

static IndexBuilder<BTreeIndex> builder("BTreeIndex");

//-----------------------------------------------------------------------------

} // namespace fdb5
