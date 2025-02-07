/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   IndexFactory.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_IndexFactory_H
#define fdb5_IndexFactory_H

#include <string>

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"

namespace eckit {
class PathName;
}

namespace fdb5 {

class BTreeIndex;

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering factory for producing IndexFactory instances

class BTreeIndexFactory {

    virtual BTreeIndex* make(const eckit::PathName& path, bool readOnly, off_t offset) const = 0;

protected:

    BTreeIndexFactory(const std::string&);
    virtual ~BTreeIndexFactory();

    std::string name_;

public:

    static void list(std::ostream&);
    static BTreeIndex* build(const std::string& name, const eckit::PathName& path, bool readOnly, off_t offset);
};

/// Templated specialisation of the self-registering factory,
/// that does the self-registration, and the construction of each object.

template <class T>
class IndexBuilder : public BTreeIndexFactory {

    BTreeIndex* make(const eckit::PathName& path, bool readOnly, off_t offset) const override {
        return new T(path, readOnly, offset);
    }

public:

    IndexBuilder(const std::string& name) : BTreeIndexFactory(name) {}
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
