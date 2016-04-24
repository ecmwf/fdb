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

        eckit::PathName tmp = eckit::PathName::unique(filePath()) + ".toc";

        /* Create TOC*/
        int iomode = O_WRONLY | O_CREAT | O_EXCL;
        SYSCALL2(fd_ = ::open( tmp.asString().c_str(), iomode, (mode_t)0777 ), tmp);
        read_   = false;

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

        if(::rename(tmp.asString().c_str(), filePath().asString().c_str()) < 0) {
            if ( errno == EEXIST ) {
                eckit::Log::warning() << "TocInitialiser: " << filePath() << " already exists" << std::endl;
                tmp.unlink();
            } else {
                std::ostringstream oss;
                oss << "TocInitialiser: failed to rename " << tmp << " to " << filePath() << std::endl;
                throw eckit::FailedSystemCall(oss.str());
            }
        }

    }
}


//-----------------------------------------------------------------------------

} // namespace fdb5
