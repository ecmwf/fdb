/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_config.h"

#include "eckit/log/Log.h"

#include "fdb5/LibFdb.h"

#include "fdb5/dist/DistDBWriter.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


DistDBWriter::DistDBWriter(const Key &key, const eckit::Configuration& config) :
    DistDB(key, config) {}


DistDBWriter::DistDBWriter(const eckit::PathName &directory, const eckit::Configuration& config) :
    DistDB(directory, config) {}


DistDBWriter::~DistDBWriter() {}

void DistDBWriter::print(std::ostream& out) const {
    out << "DistDBWriter()";
}

bool DistDBWriter::selectIndex(const Key &key) {
    NOTIMP;
}

void DistDBWriter::deselectIndex() {
    NOTIMP;
}


static DBBuilder<DistDBWriter> builder("dist.writer", false, true);

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
