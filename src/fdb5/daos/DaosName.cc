/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/utils/Tokenizer.h"
#include "eckit/filesystem/PathName.h"
// #include "eckit/io/DataHandle.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosHandle.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosName::DaosName(const std::string& pool, const std::string& cont, const std::string& oid) : pool_(pool), cont_(cont), oid_(oid) {}

DaosName::DaosName(const std::string& name) {

    // TODO: find format for documenting inputs
    // Inputs:
    //   name: [pool:]cont[:oidhi.oidlo]
    
    // TODO: replace : separator by /?

    eckit::Tokenizer parse(":");
    std::vector<std::string> bits;
    parse(name, bits);

    ASSERT(bits.size() == 3);

    pool_ = bits[0];
    cont_ = bits[1];
    oid_ = bits[2];

}

DaosName::DaosName(const eckit::URI& uri) : DaosName(uri.name()) {}

void DaosName::createManagedObject() {

    if (obj_.get() == nullptr) obj_ = std::unique_ptr<fdb5::DaosObject>(new fdb5::DaosObject(*this));

}

daos_size_t DaosName::size() {

    createManagedObject();
    return obj_->size();

}

std::string DaosName::asString() const {

    return pool_ + ':' + cont_ + ':' + oid_;

}

eckit::URI DaosName::URI() const {

    return eckit::URI("daos", eckit::PathName(asString()));

}

std::string DaosName::poolName() const {

    return pool_;
    
}

std::string DaosName::contName() const {

    return cont_;
    
}

std::string DaosName::oid() const {

    return oid_;
    
}

eckit::DataHandle* DaosName::dataHandle(bool overwrite) const {

    return new fdb5::DaosHandle(*this);

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5