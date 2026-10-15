// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   IndexFactory.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include <string>

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

