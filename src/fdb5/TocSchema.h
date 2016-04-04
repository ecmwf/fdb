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

#include "fdb5/Schema.h"
#include "fdb5/Key.h"

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

class TocSchema : public Schema {

public: // methods

    TocSchema(const Key& dbKey);
    
    virtual ~TocSchema();

    bool matchTOC(const Key& tocKey_) const;

private: // members

    Key dbKey_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif
