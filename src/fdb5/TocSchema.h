/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocSchema.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_TocSchema_H
#define fdb_TocSchema_H

#include "eckit/filesystem/PathName.h"

#include "fdb5/Index.h"
#include "fdb5/Schema.h"
#include "fdb5/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TocSchema : public Schema {

public: // methods

    TocSchema(const Key& dbKey);
    
    virtual ~TocSchema();

    bool matchTOC(const Key& tocKey) const;

    eckit::PathName tocPath() const;

    std::string dataFilePrefix(const Key& userKey) const;

    eckit::PathName generateIndexPath(const Key& userKey) const;

    eckit::PathName generateDataPath(const Key& userKey) const;

    std::string tocEntry(const Key& userKey) const;

    Index::Key dataIdx(const Key& userKey) const;

private: // members

    Key dbKey_;

    std::string     indexType_;  ///< type of the indexes in the managed Toc

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
