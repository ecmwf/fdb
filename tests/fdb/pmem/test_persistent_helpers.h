/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   April 2016

#ifndef pmem_test_persistent_helpers_h
#define pmem_test_persistent_helpers_h

#include "eckit/filesystem/PathName.h"
#include "eckit/memory/ScopedPtr.h"

#include "pmem/AtomicConstructor.h"
#include "pmem/PersistentPool.h"

#include <vector>

//----------------------------------------------------------------------------------------------------------------------

/// A fixture to obtain a unique name for this pool.

struct UniquePool {

    UniquePool() :
        path_("") {

        const char* tmpdir = ::getenv("TMPDIR");
        if (!tmpdir)
            tmpdir = ::getenv("SCRATCHDIR");
        if (!tmpdir)
            tmpdir = "/tmp";

        eckit::PathName basepath(std::string(tmpdir) + "/pool");
        path_ = eckit::PathName::unique(basepath);
    }
    ~UniquePool() {}

    eckit::PathName path_;
};


/// Some standard constants to assist testing

const size_t auto_pool_size = 1024 * 1024 * 20;
const std::string auto_pool_name = "pool-name";

/// A structure to automatically create and clean up a pool (used except in the tests where this
/// functionality is being directly tested

struct AutoPool {

    AutoPool(const pmem::AtomicConstructorBase& constructor) :
        path_(UniquePool().path_),
        pool_(path_, auto_pool_size, auto_pool_name, constructor) {}
    ~AutoPool() { pool_.remove(); }

    eckit::PathName path_;
    pmem::PersistentPool pool_;
};



/// A mock type, to call AtomicConstructors on appropriately sized regions of memory without going via the
/// nvml library

template <typename T>
class PersistentMock {

public: // methods

    PersistentMock(const pmem::AtomicConstructor<T>& ctr) :
        data_(ctr.size()) {
        ctr.make(object());
    }

    PersistentMock() :
        ctr_(new pmem::AtomicConstructor0<T>),
        data_(ctr_->size()) {
        ctr_->make(object());
    }

    template <typename X1>
    PersistentMock(const X1& x1) :
        ctr_(new pmem::AtomicConstructor1<T, X1>(x1)),
        data_(ctr_->size()) {
        ctr_->make(object());
    }

    template <typename X1, typename X2>
    PersistentMock(const X1& x1, const X2& x2) :
        ctr_(new pmem::AtomicConstructor2<T, X1, X2>(x1, x2)),
        data_(ctr_->size()) {
        ctr_->make(object());
    }

    T& object() {
        return *reinterpret_cast<T*>(&data_[0]);
    }

private: // data

    eckit::ScopedPtr<pmem::AtomicConstructor<T> > ctr_;
    std::vector<char> data_;
};

//----------------------------------------------------------------------------------------------------------------------

#endif // pmem_test_persistent_helpers_h
