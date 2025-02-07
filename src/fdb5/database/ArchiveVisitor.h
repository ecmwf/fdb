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

#include "fdb5/api/helpers/Callback.h"
#include "fdb5/database/BaseArchiveVisitor.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Archiver;

//----------------------------------------------------------------------------------------------------------------------

class ArchiveVisitor : public BaseArchiveVisitor {

public:  // methods

    ArchiveVisitor(Archiver& owner, const Key& dataKey, const void* data, size_t size,
                   const ArchiveCallback& callback = CALLBACK_ARCHIVE_NOOP);

protected:  // methods

    bool selectDatum(const Key& datumKey, const Key& fullKey) override;

    void print(std::ostream& out) const override;

private:  // methods

    void callbacks(fdb5::CatalogueWriter* catalogue, const Key& idxKey, const Key& datumKey,
                   std::shared_ptr<std::promise<std::shared_ptr<const FieldLocation>>> p,
                   std::shared_ptr<const FieldLocation> fieldLocation);

private:  // members

    const void* data_;
    size_t size_;

    const ArchiveCallback& callback_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
