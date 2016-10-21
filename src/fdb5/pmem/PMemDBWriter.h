/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   PMemDBWriter.h
/// @author Simon Smart
/// @date   Mar 2016

#ifndef fdb5_PMemDBWriter_H
#define fdb5_PMemDBWriter_H

#include "fdb5/pmem/PMemDB.h"

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class PMemDBWriter : public PMemDB {

public: // methods

    PMemDBWriter(const Key &key);

    virtual ~PMemDBWriter();

    virtual bool selectIndex(const Key &key);

private: // methods

    virtual void print( std::ostream &out ) const;
    
private: // members
    
    

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif
