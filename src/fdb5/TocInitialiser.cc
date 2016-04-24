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

    if ( !dir_.isDir() )
        throw eckit::UnexpectedState( dir_ + " is not a directory" );

    /// @TODO copy the fdb schema into the directory

    if ( !filePath().exists() ) {

        /* Copy rules first */

        eckit::Log::info() << "Copy schema from "
                           << MasterConfig::instance().schemaPath()
                           << " to "
                           << dir_ / "schema"
                           << std::endl;

        eckit::FileHandle in(MasterConfig::instance().schemaPath());
        eckit::FileHandle out(dir_ / "schema");
        in.saveInto(out);

        /* Create TOC*/
        int iomode = O_WRONLY | O_CREAT | O_EXCL;
        fd_ = ::open( filePath().asString().c_str(), iomode, (mode_t)0777 );
        read_   = false;

        if ( fd_ >= 0 ) { // successfully created
            TocRecord r = makeRecordTocInit();
            append(r);
            close();
        } else {
            if ( errno == EEXIST ) {
                eckit::Log::warning() << "TocInitialiser: " << filePath() << " already exists" << std::endl;
            } else {
                SYSCALL2(fd_, filePath());
            }
        }

    }
}


//-----------------------------------------------------------------------------

} // namespace fdb5
