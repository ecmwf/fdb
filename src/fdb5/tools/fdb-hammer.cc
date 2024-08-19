/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <unordered_set>
#include <memory>

#include "eccodes.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/StdFile.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"
#include "eckit/thread/ThreadPool.h"

#include "fdb5/message/MessageArchiver.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/tools/FDBTool.h"

// This list is currently sufficient to get to nparams=200 of levtype=ml,type=fc
const std::unordered_set<size_t> AWKWARD_PARAMS {11, 12, 13, 14, 15, 16, 49, 51, 52, 61, 121, 122, 146, 147, 169, 175, 176, 177, 179, 189, 201, 202};


using namespace eckit;


class FDBWrite : public fdb5::FDBTool {

    virtual void usage(const std::string &tool) const override;

    virtual void init(const eckit::option::CmdArgs &args) override;

    virtual int minimumPositionalArguments() const override { return 1; }

    virtual void execute(const eckit::option::CmdArgs &args) override;

    void executeRead(const eckit::option::CmdArgs& args);
    void executeWrite(const eckit::option::CmdArgs& args);

public:

    FDBWrite(int argc, char **argv) :
        fdb5::FDBTool(argc, argv),
        verbose_(false) {

        options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "Reset expver on data"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("class", "Reset class on data"));
        options_.push_back(new eckit::option::SimpleOption<bool>("statistics", "Report statistics after run"));
        options_.push_back(new eckit::option::SimpleOption<bool>("read", "Read rather than write the data"));
        options_.push_back(new eckit::option::SimpleOption<long>("nsteps", "Number of steps"));
        options_.push_back(new eckit::option::SimpleOption<long>("nensembles", "Number of ensemble members"));
        options_.push_back(new eckit::option::SimpleOption<long>("number", "The first ensemble number to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nlevels", "Number of levels"));
        options_.push_back(new eckit::option::SimpleOption<long>("nparams", "Number of parameters"));
        options_.push_back(new eckit::option::SimpleOption<bool>("verbose", "Print verbose output"));
    }
    ~FDBWrite() override {}

private:
    bool verbose_;
};

void FDBWrite::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " [--statistics] [--read] --nsteps=<nsteps> --nensembles=<nensembles> --nlevels=<nlevels> --nparams=<nparams> --expver=<expver> <grib_path>" << std::endl;
    fdb5::FDBTool::usage(tool);
}

void FDBWrite::init(const eckit::option::CmdArgs& args)
{
    FDBTool::init(args);

    ASSERT(args.has("expver"));
    ASSERT(args.has("class"));
    ASSERT(args.has("nlevels"));
    ASSERT(args.has("nsteps"));
    ASSERT(args.has("nparams"));

    verbose_ = args.getBool("verbose", false);
}

void FDBWrite::execute(const eckit::option::CmdArgs &args) {

    if (args.getBool("read", false)) {
        executeRead(args);
    } else {
        executeWrite(args);
    }
}


// class for writing a chunk of the user buffer - used to perform multiple simultaneous writes
class Init : public eckit::ThreadPoolTask {

public:
    Init(fdb5::FDB** fdb) : fdb_(fdb) {}

    void execute() {
        (*fdb_) = new fdb5::FDB();
    }

private:
    fdb5::FDB** fdb_;
};

// class for writing a chunk of the user buffer - used to perform multiple simultaneous writes
class Archive : public eckit::ThreadPoolTask {

public:
    Archive(fdb5::FDB* fdb, const char* buffer, size_t size) : fdb_(fdb), size_(size) {
        buffer_ = new char[size];
        ::memcpy(buffer_, buffer, size);
    }

    void execute() {
        fdb_->archive(buffer_, size_);
        fdb_->flush();

        delete[] buffer_;
    }

private:
    fdb5::FDB* fdb_;
    char* buffer_;
    size_t size_;
};

class Retrieve : public eckit::ThreadPoolTask {

public:
    Retrieve(const eckit::option::CmdArgs& args, fdb5::HandleGatherer& handles, metkit::mars::MarsRequest request)
        : args_(args), handles_(handles), request_(request) {
    }

    void execute() {
        fdb5::FDB fdb(args_);
        handles_.add(fdb.retrieve(request_));
    }

private:
    const eckit::option::CmdArgs& args_;
    fdb5::HandleGatherer& handles_;
    metkit::mars::MarsRequest request_;
};

void FDBWrite::executeWrite(const eckit::option::CmdArgs &args) {

    eckit::AutoStdFile fin(args(0));

    int err;
    codes_handle* handle = codes_handle_new_from_file(nullptr, fin, PRODUCT_GRIB, &err);
    ASSERT(handle);

    size_t nsteps = args.getLong("nsteps");
    size_t nensembles = args.getLong("nensembles", 1);
    size_t nlevels = args.getLong("nlevels");
    size_t nparams = args.getLong("nparams");
    size_t number  = args.getLong("number", 1);


    const char* buffer = nullptr;
    size_t size = 0;

    // fdb5::MessageArchiver archiver(fdb5::Key(), false, verbose_, args);

    std::string expver = args.getString("expver");
    size = expver.length();
    CODES_CHECK(codes_set_string(handle, "expver", expver.c_str(), &size), 0);
    std::string cls = args.getString("class");
    size = cls.length();
    CODES_CHECK(codes_set_string(handle, "class", cls.c_str(), &size), 0);

    eckit::Timer timer;
    eckit::Timer gribTimer;
    double elapsed_grib = 0;
    size_t writeCount = 0;
    size_t bytesWritten = 0;

    static size_t numThreads = 32;
    timer.start();
    eckit::ThreadPool init("init", numThreads);
    fdb5::FDB *fdb_[numThreads];
    for (size_t f = 0; f< numThreads; f++) {
        init.push(new Init(&(fdb_[f])));
    }
    init.wait();

    std::cout << "###################### INIT completed #######################" << std::endl;

    
    eckit::ThreadPool pool("hammer", numThreads);

    size_t i = 0;

    for (size_t member = 0; member < nensembles; ++member) {
        if (args.has("nensembles")) {
            CODES_CHECK(codes_set_long(handle, "number", member+number), 0);
        }
        for (size_t step = 0; step < nsteps; ++step) {
            CODES_CHECK(codes_set_long(handle, "step", step), 0);
            for (size_t level = 1; level <= nlevels; ++level) {
                CODES_CHECK(codes_set_long(handle, "level", level), 0);
                for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
                    // GRIB API only allows us to use certain parameters
                    while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
                        real_param++;
                    }

                    Log::info() << "Member: " << member
                                << ", step: " << step
                                << ", level: " << level
                                << ", param: " << real_param << std::endl;

                    CODES_CHECK(codes_set_long(handle, "param", real_param), 0);

                    CODES_CHECK(codes_get_message(handle, reinterpret_cast<const void**>(&buffer), &size), 0);

                    gribTimer.stop();
                    elapsed_grib += gribTimer.elapsed();

                    pool.push(new Archive(fdb_[i], buffer, size));
                    i = (i+1) % 4;


                    // archiver.archive(dh);
                    writeCount++;
                    bytesWritten += size;

                    gribTimer.start();
                }
            }

            gribTimer.stop();
            elapsed_grib += gribTimer.elapsed();
            // archiver.flush();
            gribTimer.start();
        }
    }

    gribTimer.stop();
    elapsed_grib += gribTimer.elapsed();
    
    pool.wait();

    timer.stop();

    codes_handle_delete(handle);

    Log::info() << "Fields written: " << writeCount << std::endl;
    Log::info() << "Bytes written: " << bytesWritten << std::endl;
    Log::info() << "Total duration: " << timer.elapsed() << std::endl;
    Log::info() << "GRIB duration: " << elapsed_grib << std::endl;
    Log::info() << "Writing duration: " << timer.elapsed() - elapsed_grib << std::endl;
    Log::info() << "Total rate: " << double(bytesWritten) / timer.elapsed() << " bytes / s" << std::endl;
    Log::info() << "Total rate: " << double(bytesWritten) / (timer.elapsed() * 1024 * 1024) << " MB / s" << std::endl;
}


void FDBWrite::executeRead(const eckit::option::CmdArgs &args) {


    fdb5::MessageDecoder decoder;
    std::vector<metkit::mars::MarsRequest> requests = decoder.messageToRequests(args(0));

    ASSERT(requests.size() == 1);
    metkit::mars::MarsRequest request = requests[0];

    size_t nsteps = args.getLong("nsteps");
    size_t nensembles = args.getLong("nensembles", 1);
    size_t nlevels = args.getLong("nlevels");
    size_t nparams = args.getLong("nparams");

    request.setValue("expver", args.getString("expver"));
    request.setValue("class", args.getString("class"));

    eckit::Timer timer;
    timer.start();

    fdb5::HandleGatherer handles(false);
    eckit::ThreadPool pool("hammerRead", 80);

    // fdb5::FDB fdb(args);
    size_t fieldsRead = 0;

    for (size_t member = 1; member <= nensembles; ++member) {
        if (args.has("nensembles")) {
            request.setValue("number", member);
        }
        for (size_t step = 0; step < nsteps; ++step) {
            request.setValue("step", step);
            for (size_t level = 1; level <= nlevels; ++level) {
                request.setValue("level", level);
                for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
                    // GRIB API only allows us to use certain parameters
                    while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
                        real_param++;
                    }
                    request.setValue("param", real_param);

                    Log::info() << "Member: " << member
                                << ", step: " << step
                                << ", level: " << level
                                << ", param: " << real_param << std::endl;

                    pool.push(new Retrieve(args, handles, request));
                    // handles.add(fdb.retrieve(request));
                    fieldsRead++;
                }
            }
        }
    }
    pool.wait();

    std::unique_ptr<eckit::DataHandle> dh(handles.dataHandle());

    EmptyHandle nullOutputHandle;
    size_t total = dh->saveInto(nullOutputHandle);

    timer.stop();

    Log::info() << "Fields read: " << fieldsRead << std::endl;
    Log::info() << "Bytes read: " << total << std::endl;
    Log::info() << "Total duration: " << timer.elapsed() << std::endl;
    Log::info() << "Total rate: " << double(total) / timer.elapsed() << " bytes / s" << std::endl;
    Log::info() << "Total rate: " << double(total) / (timer.elapsed() * 1024 * 1024) << " MB / s" << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWrite app(argc, argv);
    return app.start();
}

