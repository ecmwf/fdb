/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fcntl.h>

#include "eckit/io/FileHandle.h"

#include "fdb5/TocInitialiser.h"

#include "fdb5/MasterConfig.h"


namespace fdb5 {

//-----------------------------------------------------------------------------

TocInitialiser::TocInitialiser(const eckit::PathName &dir) : TocHandler(dir) {

    if ( !dir_.exists() ) {
        dirPath().mkdir();
    }

    int iomode = O_CREAT | O_RDWR;
    fd_ = ::open( filePath().asString().c_str(), iomode, (mode_t)0777 );

    TocRecord r;

    size_t len = readNext(r);
    if(len == 0) {

        /* Copy rules first */

        eckit::PathName schemaPath(dir_ / "schema");

        eckit::Log::info() << "Copy schema from "
                           << MasterConfig::instance().schemaPath()
                           << " to "
                           << schemaPath
                           << std::endl;

        eckit::PathName tmp = eckit::PathName::unique(schemaPath);

        eckit::FileHandle in(MasterConfig::instance().schemaPath());
        eckit::FileHandle out(tmp);
        in.saveInto(out);

        eckit::PathName::rename(tmp, schemaPath);

        read_   = false;
        TocRecord r = makeRecordTocInit();
        append(r);
    }
    else {
        ASSERT(r.head_.tag_ == TOC_INIT);
    }

    close();
}

//-----------------------------------------------------------------------------

} // namespace fdb5
