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

#ifndef fdb5_PMemDBReader_H
#define fdb5_PMemDBReader_H

#include "fdb5/pmem/PMemDB.h"

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB bases on persistent memory devices

class PMemDBReader : public PMemDB {

public: // methods

    PMemDBReader(const Key &key, const eckit::Configuration& config);
    PMemDBReader(const eckit::PathName& directory, const eckit::Configuration& config);

    virtual bool open();
    virtual void axis(const std::string& keyword, eckit::StringSet& s) const;

    virtual ~PMemDBReader() override;

    virtual bool selectIndex(const Key &key);
    virtual eckit::DataHandle* retrieve(const Key &key) const;

    virtual std::vector<Index> indexes(bool sorted) const;

    virtual StatsReportVisitor* statsReportVisitor();

private: // methods

    virtual void print( std::ostream &out ) const override;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_PMemDBReader_H
