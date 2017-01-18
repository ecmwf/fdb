/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/DataHandle.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"
#include "fdb5/grib/GribArchiver.h"
#include "fdb5/tools/FDBAccess.h"
#include "fdb5/config/UMask.h"

class Grib2Fdb5 : public fdb5::FDBAccess {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int minimumPositionalArguments() const {
        return 1;
    }

  public:
    Grib2Fdb5(int argc, char **argv): fdb5::FDBAccess(argc, argv) {}
};

void Grib2Fdb5::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " [-d database] [-c class] [-e expver] [-T type] [-s stream] [-f file]" << std::endl;
    fdb5::FDBAccess::usage(tool);
}

void Grib2Fdb5::execute(const eckit::option::CmdArgs &args) {

    fdb5::UMask umask(fdb5::UMask::defaultUMask());

    fdb5::GribArchiver archiver;
    fdb5::Key check;

    size_t i = 0;
    while (i + 1 < args.count()) {
        std::string k = args(i);

        if (k == "-f") {
            eckit::PathName path(args(i + 1));
            std::cout << "Processing " << path << std::endl;
            std::cout << "Key " << check << std::endl;
            eckit::ScopedPtr<eckit::DataHandle> dh ( path.fileHandle() );
            archiver.archive( *dh );
            i += 2;
            continue;
        }

        if (k == "-d") {
            std::cout << "Option -d ignored: " << args(i + 1) << std::endl;
            i += 2;
            continue;
        }

        if (k == "-1") {
            std::cout << "Option -1 ignored" <<  std::endl;
            i++;
            continue;
        }

        if (k == "-e") {
            check.set("expver", args(i + 1));
            i += 2;
            continue;
        }

        if (k == "-c") {
            check.set("class", args(i + 1));
            i += 2;
            continue;
        }

        if (k == "-s") {
            check.set("stream", args(i + 1));
            i += 2;
            continue;
        }

        if (k == "-T") {
            check.set("type", args(i + 1));
            i += 2;
            continue;
        }

        usage(args.tool());
        exit(1);
    }

}


int main(int argc, char **argv) {
    Grib2Fdb5 app(argc, argv);
    return app.start();
}

