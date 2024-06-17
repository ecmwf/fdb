/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
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

/// DB that implements the FDB bases on persistent memory devices

class PMemDBWriter : public PMemDB {

public: // methods

    PMemDBWriter(const CanonicalKey& key, const eckit::Configuration& config);
    PMemDBWriter(const eckit::PathName& directory, const eckit::Configuration& config);

    virtual ~PMemDBWriter() override;

    virtual bool selectIndex(const CanonicalKey& idxKey);
    virtual void close();
    virtual void archive(const CanonicalKey& key, const void *data, eckit::Length length);

private: // methods

    virtual void print( std::ostream &out ) const override;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif
