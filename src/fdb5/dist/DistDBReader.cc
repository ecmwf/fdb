/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/LibFdb.h"
#include "fdb5/dist/DistDBReader.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DistDBReader::DistDBReader(const Key& key, const eckit::Configuration& config) :
    DistDB(key, config) {}

DistDBReader::DistDBReader(const eckit::PathName& directory, const eckit::Configuration& config) :
    DistDB(directory, config) {}


DistDBReader::~DistDBReader() {}


void DistDBReader::print(std::ostream& out) const {
    out << "DistDBReader()";
}

bool DistDBReader::selectIndex(const Key &key) {
    NOTIMP;
}

void DistDBReader::deselectIndex() {
    NOTIMP;
}


static DBBuilder<DistDBReader> builder("dist.reader", true, false);

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
