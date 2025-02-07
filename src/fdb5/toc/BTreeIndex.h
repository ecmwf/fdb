/*
 * (C) Copyright 1996- ECMWF.
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

#ifndef fdb5_BTreeIndex_H
#define fdb5_BTreeIndex_H

#include "eckit/eckit.h"

#include "eckit/container/BTree.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"

#include "eckit/types/FixedString.h"

#include "fdb5/database/Index.h"

namespace fdb5 {

class FieldRef;

//----------------------------------------------------------------------------------------------------------------------

class BTreeIndexVisitor {
public:

    virtual ~BTreeIndexVisitor();
    virtual void visit(const std::string& key, const FieldRef&) = 0;
};

class BTreeIndex {
public:

    virtual ~BTreeIndex();
    virtual bool get(const std::string& key, FieldRef& data) const = 0;
    virtual bool set(const std::string& key, const FieldRef& data) = 0;
    virtual void flush()                                           = 0;
    virtual void sync()                                            = 0;
    virtual void visit(BTreeIndexVisitor& visitor) const           = 0;
    virtual void flock()                                           = 0;
    virtual void funlock()                                         = 0;
    virtual void preload()                                         = 0;


    static const std::string& defaulType();
};

//----------------------------------------------------------------------------------------------------------------------

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
class BTreeIndexBuilder : public BTreeIndexFactory {

    BTreeIndex* make(const eckit::PathName& path, bool readOnly, off_t offset) const override {
        return new T(path, readOnly, offset);
    }

public:

    BTreeIndexBuilder(const std::string& name) : BTreeIndexFactory(name) {}
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
