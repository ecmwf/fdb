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
/// @date Jun 2024

#pragma once

#include "eckit/exception/Exceptions.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosAsyncKeyValue.h"

#include "fdb5/database/IndexLocation.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RadosIndexLocation : public IndexLocation {

public: // methods

// #if defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE) || defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
//     RadosIndexLocation(const eckit::RadosPersistentKeyValue& name, off_t offset);

//     const eckit::RadosPersistentKeyValue& radosName() const { return name_; };
// #else
    RadosIndexLocation(const eckit::RadosKeyValue& name, off_t offset);

    const eckit::RadosKeyValue& radosName() const { return name_; };
// #endif

    eckit::URI uri() const override { return name_.uri(); }

    IndexLocation* clone() const override { NOTIMP; }

protected: // For Streamable

    void encode(eckit::Stream&) const override { NOTIMP; }

private: // methods

    void print(std::ostream &out) const override;

private: // members

// #if defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE) || defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
//     eckit::RadosPersistentKeyValue name_;
// #else
    eckit::RadosKeyValue name_;
// #endif

    off_t offset_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
