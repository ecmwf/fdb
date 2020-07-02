/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Progress.h"

#include "metkit/data/Reader.h"
#include "metkit/data/Message.h"

#include "fdb5/grib/GribIndexer.h"
#include "fdb5/toc/AdoptVisitor.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GribIndexer::GribIndexer(bool checkDuplicates) :
    GribDecoder(checkDuplicates) {
}

void GribIndexer::index(const eckit::PathName &path) {
    eckit::Timer timer("fdb::service::archive");

    metkit::data::Reader reader(path);

    size_t count = 0;
    eckit::Length total_size = 0;

    eckit::Length totalFileSize = path.size();

    eckit::Progress progress("Scanning", 0, totalFileSize);

    Key key;

    eckit::PathName full(path.realName());

    metkit::data::Message msg;
    while ( (msg = reader.next()) ) {
       gribToKey(msg, key);

        // eckit::Log::info() << key << std::endl;

        eckit::Length length = msg.length();
        eckit::Offset offset = reader.position() - length;

        AdoptVisitor visitor(*this, key, full, offset, length);

        archive(key, visitor);

        total_size += length;
        progress(total_size);
        count++;
    }

    eckit::Log::info() << "FDB indexer " << eckit::Plural(count, "field") << ","
                       << " size " << eckit::Bytes(total_size) << ","
                       << " in " << eckit::Seconds(timer.elapsed())
                       << " (" << eckit::Bytes(total_size, timer) << ")" <<  std::endl;

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
