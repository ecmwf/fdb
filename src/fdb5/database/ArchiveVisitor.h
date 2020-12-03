/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ArchiveVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_ArchiveVisitor_H
#define fdb5_ArchiveVisitor_H

#include "fdb5/database/BaseArchiveVisitor.h"

namespace metkit { class MarsRequest; }

namespace fdb5 {

class Archiver;

//----------------------------------------------------------------------------------------------------------------------

class ArchiveVisitor : public BaseArchiveVisitor {

public: // methods

    ArchiveVisitor(Archiver &owner, const Key &field, const void *data, size_t size);

protected: // methods

    virtual bool selectDatum(const Key &key, const Key &full) override;

    virtual void print( std::ostream &out ) const override;

private: // members

    const void *data_;
    size_t size_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
