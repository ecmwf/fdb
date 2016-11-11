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

#ifndef fdb5_PMemDBReader_H
#define fdb5_PMemDBReader_H

#include "fdb5/pmem/PMemDB.h"

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class PMemDBReader : public PMemDB {

protected: // types

    typedef std::vector<Index*> IndexVector;

public: // methods

    PMemDBReader(const Key &key);

    virtual ~PMemDBReader();

    virtual bool selectIndex(const Key &key);

private: // methods

    virtual void print( std::ostream &out ) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_PMemDBReader_H
