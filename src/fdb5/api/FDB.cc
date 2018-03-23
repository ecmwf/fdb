/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDB::FDB(const Config &config) :
    internal_(FDBFactory::build(config)) {}


FDB::~FDB() {}

void FDB::archive(const Key& key, const void* data, size_t length) {
    internal_->archive(key, data, length);

}

eckit::DataHandle *FDB::retrieve(const MarsRequest& request) {
    return internal_->retrieve(request);
}

void FDB::flush() {
    internal_->flush();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
