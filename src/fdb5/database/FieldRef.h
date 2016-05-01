/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_FieldRef_H
#define fdb5_FieldRef_H

#include "eckit/eckit.h"

#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/io/Offset.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/database/FieldDetails.h"

namespace fdb5 {

class Field;
class FileStore;

class FieldRefBase {

  public:
    typedef size_t PathID;

    FieldRefBase();
    FieldRefBase(const FileStore &, const Field &);


    PathID pathId() const;
    const eckit::Offset &offset() const;
    const eckit::Length &length() const;

  protected:
    PathID          pathId_;
    eckit::Offset   offset_;
    eckit::Length   length_;

    void print(std::ostream &s) const;

    friend std::ostream &operator<<(std::ostream &s, const FieldRefBase &x) {
        x.print(s);
        return s;
    }
};


class FieldRef : public FieldRefBase, public FieldDetails {
public:
    FieldRef();
    explicit FieldRef(const FileStore &, const Field &);

    FieldRef(const FieldRefBase&);
    // FieldRef(const FieldRef&);

    FieldRef& operator=(const FieldRefBase&);
    // FieldRef& operator=(const FieldRef&);

};

typedef FieldRef FieldRefFull;

class FieldRefReduced : public FieldRefBase {
public:
    FieldRefReduced();
    FieldRefReduced(const FieldRef&);
};



} // namespace fdb5

#endif
