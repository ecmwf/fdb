// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   ArchiveVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include "fdb5/api/helpers/Callback.h"
#include "fdb5/database/BaseArchiveVisitor.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Archiver;

//----------------------------------------------------------------------------------------------------------------------

class ArchiveVisitor : public BaseArchiveVisitor, public std::enable_shared_from_this<ArchiveVisitor> {

public:  // methods

    static std::shared_ptr<ArchiveVisitor> create(Archiver& owner, const Key& dataKey, const void* data, size_t size,
                                                  const ArchiveCallback& callback = CALLBACK_ARCHIVE_NOOP);

protected:  // methods

    ArchiveVisitor(Archiver& owner, const Key& dataKey, const void* data, size_t size, const ArchiveCallback& callback);

    bool selectDatum(const Key& datumKey, const Key& fullKey) override;

    void print(std::ostream& out) const override;

private:  // methods

    void callbacks(std::shared_ptr<CatalogueWriter> catalogue, const Key& idxKey, const Key& datumKey,
                   std::shared_ptr<std::promise<std::shared_ptr<const FieldLocation>>> p,
                   std::shared_ptr<const FieldLocation> fieldLocation);

private:  // members

    const void* data_;
    size_t size_;

    const ArchiveCallback& callback_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

