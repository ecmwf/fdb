/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/DataHandle.h"
#include "eckit/log/Timer.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Progress.h"

#include "grib_api.h"

#include "marslib/EmosFile.h"

#include "fdb5/Key.h"
#include "fdb5/GribIndexer.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GribIndexer::GribIndexer(bool checkDuplicates) :
    GribDecoder(checkDuplicates)
{
}

void GribIndexer::index(const eckit::PathName& path)
{
    Timer timer("fdb::service::archive");

    EmosFile file(path);
    size_t len = 0;

    std::set<Key> seen;

    size_t count = 0;
    Length total_size = 0;

    Length totalFileSize = path.size();

    Progress progress("Scanning", 0, totalFileSize);

    Key key;


    while( (len = gribToKey(file, key))  )
    {

        Log::info() << key << std::endl;

        Length length = len;
        Offset offset = file.position() - length;

        adopt(key, path, offset, length); // now index it

        total_size += len;
        progress(total_size);
        count++;
    }

    Log::info() << "FDB indexer " << BigNum(count) << " fields,"
                << " size " << Bytes(total_size) << ","
                << " in " << Seconds(timer.elapsed())
                << " (" << Bytes(total_size, timer) << ")" <<  std::endl;

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
