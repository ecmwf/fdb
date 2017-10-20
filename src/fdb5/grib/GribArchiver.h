/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   GribArchiver.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_GribArchiver_H
#define fdb5_GribArchiver_H

#include "fdb5/database/Archiver.h"
#include "fdb5/grib/GribDecoder.h"
#include "fdb5/database/Key.h"

#include "eckit/io/Length.h"

namespace eckit {
class DataHandle;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class GribArchiver :
    public Archiver,
    public GribDecoder {

public: // methods

    GribArchiver(const fdb5::Key& key = Key(), bool completeTransfers = false);

    void filters(const std::string& include, const std::string& exclude) {
        include_ = Key(include);
        exclude_ = Key(exclude);
    }

    eckit::Length archive(eckit::DataHandle &source);

private: // protected

    bool filter(const Key& k) const;

private: // members

    fdb5::Key key_;

    fdb5::Key include_;
    fdb5::Key exclude_;

    bool completeTransfers_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
