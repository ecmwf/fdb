/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Mar 2023

#pragma once

// #include <sys/types.h>

#include "eckit/exception/Exceptions.h"
// #include "eckit/filesystem/PathName.h"
// #include "eckit/filesystem/URI.h"

#include "fdb5/database/IndexLocation.h"

#include "fdb5/daos/DaosName.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosIndexLocation : public IndexLocation {

public: // methods

    DaosIndexLocation(const fdb5::DaosKeyValueName& name, off_t offset);
    // TocIndexLocation(eckit::Stream&);

    // off_t offset() const;

//    eckit::PathName path() const override;
    eckit::URI uri() const override { return name_.URI(); }

    IndexLocation* clone() const override { NOTIMP; }

    const fdb5::DaosKeyValueName& daosName() const { return name_; };

// public: // For Streamable

//     static const eckit::ClassSpec&  classSpec() { return classSpec_;}

protected: // For Streamable

    // const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    void encode(eckit::Stream&) const override { NOTIMP; }

    // static eckit::ClassSpec                    classSpec_;
    // static eckit::Reanimator<TocIndexLocation> reanimator_;

private: // methods

    void print(std::ostream &out) const override;

private: // members

    fdb5::DaosKeyValueName name_;

    off_t offset_;

// private: // friends

//     friend class TocIndex;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
