/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   AdoptVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_AdoptVisitor_H
#define fdb5_AdoptVisitor_H

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

#endif
