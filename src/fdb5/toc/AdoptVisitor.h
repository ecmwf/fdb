// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   AdoptVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include "eckit/filesystem/PathName.h"

#include "fdb5/database/BaseArchiveVisitor.h"

namespace metkit {
class MarsRequest;
}

namespace fdb5 {

class Archiver;

//----------------------------------------------------------------------------------------------------------------------

class AdoptVisitor : public BaseArchiveVisitor {

public:  // methods

    AdoptVisitor(Archiver& owner, const Key& initialFieldKey, const eckit::PathName& path, eckit::Offset offset,
                 eckit::Length length);

protected:  // methods

    bool selectDatum(const Key& datumKey, const Key& fullKey) override;

    void print(std::ostream& out) const override;

private:  // members

    const eckit::PathName path_;
    eckit::Offset offset_;
    eckit::Length length_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
