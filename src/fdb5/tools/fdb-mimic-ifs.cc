/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/StdFile.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"

#include "fdb5/grib/GribArchiver.h"
#include "fdb5/tools/FDBAccess.h"
#include "fdb5/config/UMask.h"

#include "metkit/grib/GribHandle.h"

#include "eccodes.h"


using namespace eckit;


class FDBWrite : public fdb5::FDBAccess {

    virtual void usage(const std::string &tool) const;

    virtual void init(const eckit::option::CmdArgs &args);

    virtual int minimumPositionalArguments() const { return 1; }

    virtual void execute(const eckit::option::CmdArgs &args);

public:

    FDBWrite(int argc, char **argv) : fdb5::FDBAccess(argc, argv) {

        options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "Reset expver on data"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("class", "Reset class on data"));
        options_.push_back(new eckit::option::SimpleOption<bool>("statistics", "Report statistics after run"));
        options_.push_back(new eckit::option::SimpleOption<long>("nsteps", "Number of steps"));
        options_.push_back(new eckit::option::SimpleOption<long>("nensembles", "Number of ensemble members"));
        options_.push_back(new eckit::option::SimpleOption<long>("nlevels", "Number of levels"));
    }
};

void FDBWrite::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " [--statistics] --nsteps=<nsteps> --nensembles=<nensembles> --nlevels=<nlevels> --expver=<expver> <grib_path>" << std::endl;
    fdb5::FDBAccess::usage(tool);
}

void FDBWrite::init(const eckit::option::CmdArgs& args)
{
    FDBAccess::init(args);

    ASSERT(args.has("expver"));
    ASSERT(args.has("class"));
    ASSERT(args.has("nlevels"));
    ASSERT(args.has("nsteps"));
}

void FDBWrite::execute(const eckit::option::CmdArgs &args) {

    eckit::AutoStdFile fin(args(0));

    int err;
    codes_handle* handle = codes_handle_new_from_file(0, fin, PRODUCT_GRIB, &err);
    ASSERT(handle != 0);

    /*long value;
    metkit::grib::GribHandle gh(*handle);
    ASSERT(gh.hasKey("number"));
    ASSERT(gh.hasKey("step"));
    ASSERT(gh.hasKey("level"));
    ASSERT(gh.hasKey("expver"));*/

    size_t nsteps = args.getLong("nsteps");
    size_t nensembles = args.getLong("nensembles", 1);
    size_t nlevels = args.getLong("nlevels");

    const char* buffer = 0;
    size_t size = 0;

    fdb5::GribArchiver archiver(fdb5::Key(), false, verbose_, args);

    std::string expver = args.getString("expver");
    size = expver.length();
    CODES_CHECK(codes_set_string(handle, "expver", expver.c_str(), &size), 0);
    std::string cls = args.getString("class");
    size = cls.length();
    CODES_CHECK(codes_set_string(handle, "class", cls.c_str(), &size), 0);

    for (size_t member = 1; member <= nensembles; ++member) {
        if (args.has("nensembles")) {
            CODES_CHECK(codes_set_long(handle, "number", member), 0);
        }
        for (size_t step = 0; step <= nsteps; ++step) {
            CODES_CHECK(codes_set_long(handle, "step", step), 0);
            for (size_t level = 1; level <= nlevels; ++level) {
                CODES_CHECK(codes_set_long(handle, "level", level), 0);

                Log::info() << "Member: " << member
                            << ", step: " << step
                            << ", level: " << level << std::endl;

                CODES_CHECK(codes_get_message(handle, reinterpret_cast<const void**>(&buffer), &size), 0);

                MemoryHandle dh(buffer, size);
                archiver.archive(dh);
            }
            archiver.flush();
        }
    }

    codes_handle_delete(handle);
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWrite app(argc, argv);
    return app.start();
}

