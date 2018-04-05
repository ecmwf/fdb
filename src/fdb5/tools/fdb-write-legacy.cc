/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/DataHandle.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/config/Resource.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"

#include "metkit/grib/MetFile.h"
#include "metkit/grib/GribDataBlob.h"

#include "fdb5/legacy/LegacyArchiver.h"
#include "fdb5/tools/FDBAccess.h"
#include "fdb5/config/UMask.h"

class FDBWriteLegacy : public fdb5::FDBAccess {

    virtual void usage(const std::string &tool) const;

    virtual void init(const eckit::option::CmdArgs &args);

    virtual int minimumPositionalArguments() const { return 1; }

    virtual void execute(const eckit::option::CmdArgs &args);

public:

    FDBWriteLegacy(int argc, char **argv) : fdb5::FDBAccess(argc, argv) {
    }
};

void FDBWriteLegacy::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " <path1> [path2] ..." << std::endl;
    fdb5::FDBAccess::usage(tool);
}

void FDBWriteLegacy::init(const eckit::option::CmdArgs& args)
{
    FDBAccess::init(args);
}

void FDBWriteLegacy::execute(const eckit::option::CmdArgs &args) {

    fdb5::legacy::LegacyArchiver archiver;

    static long gribBufferSize = eckit::Resource<long>("gribBufferSize", 64*1024*1024);
    eckit::Buffer buffer(gribBufferSize);
    long len = 0;

    for (size_t i = 0; i < args.count(); i++) {

        eckit::PathName path(args(i));
        eckit::Log::info() << "Processing " << path << std::endl;

        metkit::grib::MetFile file(path);

        size_t nMsg = 0;
        while( (len = file.readSome(buffer)) != 0 )
        {
            metkit::grib::GribMetaData metadata(buffer, len);

            std::vector<std::string> kws = metadata.keywords();

            for(std::vector<std::string>::const_iterator k = kws.begin(); k != kws.end(); ++k) {
                std::string v;
                metadata.getValue(*k,v);
                archiver.legacy(*k, v);
            }

            eckit::DataBlobPtr grib( new metkit::grib::GribDataBlob(buffer, len) );
            ++nMsg;

            archiver.archive(grib);
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWriteLegacy app(argc, argv);
    return app.start();
}
