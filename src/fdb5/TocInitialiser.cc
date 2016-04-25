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
#include "eckit/log/Seconds.h"

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

        /* Create TOC*/
        int iomode = O_WRONLY | O_CREAT | O_EXCL;
        fd_ = ::open( filePath().asString().c_str(), iomode, (mode_t)0777 );
        read_   = false;

        // TODO: what if we are killed here?

        if ( fd_ >= 0 ) { // successfully created

            /* Copy rules first */

            eckit::Log::info() << "Copy schema from "
                               << MasterConfig::instance().schemaPath()
                               << " to "
                               << dir_ / "schema"
                               << std::endl;

            eckit::FileHandle in(MasterConfig::instance().schemaPath());
            eckit::FileHandle out(dir_ / "schema");
            in.saveInto(out);

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


    // We have a race condition, wait for writer to finish

    while(filePath().size() == eckit::Length(0)) {
        long age = ::time(0) - filePath().created();
        eckit::Log::warning() << "TocInitialiser: " << filePath() << " is empty, waiting... age of file is " << eckit::Seconds(age) << std::endl;
        if(age > 5*60) {
            std::stringstream oss;
            oss << "TocInitialiser: " << filePath() << " is empty, age of file is " << eckit::Seconds(age) << ", it may need to be removed";
            throw eckit::SeriousBug(oss.str());
        }
        ::sleep(1);
    }
}


//-----------------------------------------------------------------------------

} // namespace fdb5
