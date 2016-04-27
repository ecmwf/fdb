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
/// @date Dec 2014

#ifndef fdb5_TocReverseIndexes_H
#define fdb5_TocReverseIndexes_H

#include "fdb5/Key.h"
#include "fdb5/TocHandler.h"

namespace fdb5 {

//-----------------------------------------------------------------------------

/// Caches a list of the contents of the Toc

class TocReverseIndexes : public TocHandler {

public: // methods

    TocReverseIndexes( const eckit::PathName& dir );

    std::vector< eckit::PathName > indexes(const Key& key) const;
    std::vector< eckit::PathName > indexes() const;

private:

    void init(); ///< May be called multiple times

    typedef std::map< Key, TocPaths > TocMap;

	TocVec toc_;

    mutable TocMap cacheIndexes_;

    bool inited_;
};

//-----------------------------------------------------------------------------

} // namespace fdb5

#endif
