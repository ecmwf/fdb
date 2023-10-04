/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sys/time.h>

#include <unordered_set>
#include <memory>
#include <iomanip>

#include "eccodes.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/StdFile.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"

#include "fdb5/message/MessageArchiver.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/tools/FDBTool.h"
#include "fdb5/daos/DaosSession.h"

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
        options_.push_back(new eckit::option::SimpleOption<long>("level", "The first level number to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nparams", "Number of parameters"));
        // options_.push_back(new eckit::option::SimpleOption<long>("node-id", "Identifier of the node running fdb-hammer"));
        // options_.push_back(new eckit::option::SimpleOption<long>("proc-id", "Identifier of the process running fdb-hammer"));
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

void FDBWrite::executeWrite(const eckit::option::CmdArgs &args) {

    eckit::AutoStdFile fin(args(0));

    int err;
    codes_handle* handle = codes_handle_new_from_file(nullptr, fin, PRODUCT_GRIB, &err);
    ASSERT(handle);

    size_t nsteps = args.getLong("nsteps");
    size_t nensembles = args.getLong("nensembles", 1);
    size_t nlevels = args.getLong("nlevels");
    size_t nparams = args.getLong("nparams");
    //size_t number = args.getLong("number", 1);
    size_t number = args.getLong("number", 0);
    size_t level = args.getLong("level", 1);

    // size_t node_id  = args.getLong("node-id", 0);
    // size_t proc_id  = args.getLong("proc-id", 0);

    const char* buffer = nullptr;
    size_t size = 0;

    std::vector<struct timeval> tval_before_io(nensembles * nsteps * nlevels * nparams);
    std::vector<struct timeval> tval_after_io(nensembles * nsteps * nlevels * nparams);
    struct timeval tval_result_io;

    fdb5::MessageArchiver archiver(fdb5::Key(), false, verbose_, config(args));

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

    timer.start();

    for (size_t member = 0; member < nensembles; ++member) {
        if (args.has("nensembles")) {
            CODES_CHECK(codes_set_long(handle, "number", member+number), 0);
        }
        for (size_t step = 0; step < nsteps; ++step) {
            CODES_CHECK(codes_set_long(handle, "step", step), 0);
            for (size_t lev = 1; lev <= nlevels; ++lev) {
                //CODES_CHECK(codes_set_long(handle, "level", lev+level), 0);
                CODES_CHECK(codes_set_long(handle, "level", lev+level-1), 0);
                for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
                    // GRIB API only allows us to use certain parameters
                    while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
                        real_param++;
                    }

                    // Log::info() << "Member: " << member
                    //             << ", step: " << step
                    //             << ", level: " << lev
                    //             << ", param: " << real_param << std::endl;

                    CODES_CHECK(codes_set_long(handle, "paramId", real_param), 0);

                    CODES_CHECK(codes_get_message(handle, reinterpret_cast<const void**>(&buffer), &size), 0);

                    gribTimer.stop();
                    elapsed_grib += gribTimer.elapsed();

                    MemoryHandle dh(buffer, size);
                    gettimeofday(&(tval_before_io.at(writeCount)), NULL);
                    archiver.archive(dh);
                    //gettimeofday(&(tval_after_io.at(writeCount)), NULL);
                    writeCount++;
                    bytesWritten += size;

                    gribTimer.start();
                }
            }

            gribTimer.stop();
            elapsed_grib += gribTimer.elapsed();
            archiver.flush();
            // a timestamp is kept for only the last field written in every step
            gettimeofday(&(tval_after_io.at(writeCount - 1)), NULL);
            gribTimer.start();
        }
    }

    gribTimer.stop();
    elapsed_grib += gribTimer.elapsed();

    timer.stop();

    codes_handle_delete(handle);

    for (int i = 0; i < writeCount; i++) {

        // Log::info() << "THE ITERATION IS: " << i << std::endl;

        if (i == 0) {

            std::ios oldState(nullptr);
            oldState.copyfmt(Log::info());

            Log::info() << "Timestamp before first IO: " <<
                (long int)tval_before_io[i].tv_sec << "." <<
                std::setw(6) << std::setfill('0') <<
                (long int)tval_before_io[i].tv_usec << std::endl;

            Log::info().copyfmt(oldState);

        }

        // std::string message = "";
        // message += "node " + node_id + " client " + proc_id + " rep " + i;

        // timersub(&tval_after_aclose[i], &tval_before_aopen[i], &tval_result_aopenclose);
        // printf("Profiling %sdaos_field_io daos_write - %s: %s%ld.%06ld\n",
        //       message, "daos_array_open_write_close", "\t",
        //       (long int)tval_result_aopenclose.tv_sec,
        //       (long int)tval_result_aopenclose.tv_usec);
        // printf("Profiling %sdaos_field_io daos_write - %s: %s%ld.%06ld\n",
        //       message, "daos_array_open timestamp before", "\t",
        //       (long int)tval_before_aopen[i].tv_sec,
        //       (long int)tval_before_aopen[i].tv_usec);
        // printf("Profiling %sdaos_field_io daos_write - %s: %s%ld.%06ld\n",
        //       message, "daos_array_close timestamp after", "\t",
        //       (long int)tval_after_aclose[i].tv_sec,
        //       (long int)tval_after_aclose[i].tv_usec);

        // timersub(&tval_after_io[i], &tval_before_io[i], &tval_result_io);

        // std::ios oldState(nullptr);
        // oldState.copyfmt(Log::info());

        // Log::info() << "Profiling node " << node_id << " client " << proc_id << " - fdb_hammer_write - IO wc time: " <<
        //         (long int)tval_result_io.tv_sec << "." <<
        //         std::setw(6) << std::setfill('0') <<
        //         (long int)tval_result_io.tv_usec << std::endl;

        // Log::info().copyfmt(oldState);

        if (i == (writeCount - 1)) {

            std::ios oldState(nullptr);
            oldState.copyfmt(Log::info());

            Log::info() << "Timestamp after last IO: " <<
                (long int)tval_after_io[i].tv_sec << "." <<
                std::setw(6) << std::setfill('0') <<
                (long int)tval_after_io[i].tv_usec << std::endl;

            Log::info().copyfmt(oldState);

        }

        // timersub(&tval_after_rep[i], &tval_before_rep[i], &tval_result_rep);
        // printf("Profiling node %d client %d - daos_field_write - %s: %s%ld.%06ld\n",
        //        node_id, client_id, "rep total wc time", "\t\t",
        //        (long int)tval_result_rep.tv_sec, (long int)tval_result_rep.tv_usec);

        // Log::info() << "DATA WRITTEN SUCCESSFULLY" << std::endl;
    }

    fdb5::DaosManager::instance().stats().report(std::cout);

    Log::info() << "fdb-hammer - Fields written: " << writeCount << std::endl;
    Log::info() << "fdb-hammer - Bytes written: " << bytesWritten << std::endl;
    Log::info() << "fdb-hammer - Total duration: " << timer.elapsed() << std::endl;
    Log::info() << "fdb-hammer - GRIB duration: " << elapsed_grib << std::endl;
    Log::info() << "fdb-hammer - Writing duration: " << timer.elapsed() - elapsed_grib << std::endl;
    // Log::info() << "fdb-hammer - Total rate: " << double(bytesWritten) / timer.elapsed() << " bytes / s" << std::endl;
    // Log::info() << "fdb-hammer - Total rate: " << double(bytesWritten) / (timer.elapsed() * 1024 * 1024) << " MB / s" << std::endl;
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
    size_t number = args.getLong("number", 0);
    //size_t number = args.getLong("number", 1);
    size_t level = args.getLong("level", 1);

    // size_t node_id  = args.getLong("node-id", 0);
    // size_t proc_id  = args.getLong("proc-id", 0);
    //
    request.setValue("expver", args.getString("expver"));
    request.setValue("class", args.getString("class"));

    std::vector<struct timeval> tval_before_io(nensembles * nsteps * nlevels * nparams);
    std::vector<struct timeval> tval_after_io(nensembles * nsteps * nlevels * nparams);
    struct timeval tval_result_io;

    eckit::Timer timer;
    timer.start();

    fdb5::HandleGatherer handles(false);
    fdb5::FDB fdb(config(args));
    size_t fieldsRead = 0;

    //for (size_t member = 1; member <= nensembles; ++member) {
    for (size_t member = 0; member < nensembles; ++member) {
        if (args.has("nensembles")) {
            request.setValue("number", member+number);
        }
        for (size_t step = 0; step < nsteps; ++step) {
            request.setValue("step", step);
            for (size_t lev = 1; lev <= nlevels; ++lev) {
                //request.setValue("level", lev+level);
                request.setValue("levelist", lev+level-1);
                for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
                    // GRIB API only allows us to use certain parameters
                    while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
                        real_param++;
                    }
                    request.setValue("param", real_param);

                    // Log::info() << "Member: " << member
                    //             << ", step: " << step
                    //             << ", level: " << lev
                    //             << ", param: " << real_param << std::endl;

                    gettimeofday(&(tval_before_io.at(fieldsRead)), NULL);
                    handles.add(fdb.retrieve(request));
                    fieldsRead++;
                }
            }
        }
    }

    std::unique_ptr<eckit::DataHandle> dh(handles.dataHandle());

    EmptyHandle nullOutputHandle;
    size_t total = dh->copyTo(nullOutputHandle);
    gettimeofday(&(tval_after_io.at(0)), NULL);

    timer.stop();

    for (int i = 0; i < fieldsRead; i++) {

        // Log::info() << "THE ITERATION IS: " << i << std::endl;

        if (i == 0) {

            std::ios oldState(nullptr);
            oldState.copyfmt(Log::info());

            Log::info() << "Timestamp before first IO: " <<
                (long int)tval_before_io[i].tv_sec << "." <<
                std::setw(6) << std::setfill('0') <<
                (long int)tval_before_io[i].tv_usec << std::endl;

            Log::info().copyfmt(oldState);

        }

        // std::string message = "";
        // message += "node " + node_id + " client " + proc_id + " rep " + i;

        // timersub(&tval_after_aclose[i], &tval_before_aopen[i], &tval_result_aopenclose);
        // printf("Profiling %sdaos_field_io daos_write - %s: %s%ld.%06ld\n",
        //       message, "daos_array_open_write_close", "\t",
        //       (long int)tval_result_aopenclose.tv_sec,
        //       (long int)tval_result_aopenclose.tv_usec);
        // printf("Profiling %sdaos_field_io daos_write - %s: %s%ld.%06ld\n",
        //       message, "daos_array_open timestamp before", "\t",
        //       (long int)tval_before_aopen[i].tv_sec,
        //       (long int)tval_before_aopen[i].tv_usec);
        // printf("Profiling %sdaos_field_io daos_write - %s: %s%ld.%06ld\n",
        //       message, "daos_array_close timestamp after", "\t",
        //       (long int)tval_after_aclose[i].tv_sec,
        //       (long int)tval_after_aclose[i].tv_usec);

        // timersub(&tval_after_io[i], &tval_before_io[i], &tval_result_io);

        // std::ios oldState(nullptr);
        // oldState.copyfmt(Log::info());

        // Log::info() << "Profiling node " << node_id << " client " << proc_id << " - fdb_hammer_write - IO wc time: " <<
        //         (long int)tval_result_io.tv_sec << "." <<
        //         std::setw(6) << std::setfill('0') <<
        //         (long int)tval_result_io.tv_usec << std::endl;

        // Log::info().copyfmt(oldState);

        if (i == (fieldsRead - 1)) {

            std::ios oldState(nullptr);
            oldState.copyfmt(Log::info());

            Log::info() << "Timestamp after last IO: " <<
                (long int)tval_after_io[0].tv_sec << "." <<
                std::setw(6) << std::setfill('0') <<
                (long int)tval_after_io[0].tv_usec << std::endl;

            Log::info().copyfmt(oldState);

        }

        // timersub(&tval_after_rep[i], &tval_before_rep[i], &tval_result_rep);
        // printf("Profiling node %d client %d - daos_field_write - %s: %s%ld.%06ld\n",
        //        node_id, client_id, "rep total wc time", "\t\t",
        //        (long int)tval_result_rep.tv_sec, (long int)tval_result_rep.tv_usec);

        // Log::info() << "DATA READ SUCCESSFULLY" << std::endl;
    }

    fdb5::DaosManager::instance().stats().report(std::cout);

    Log::info() << "fdb-hammer - Fields read: " << fieldsRead << std::endl;
    Log::info() << "fdb-hammer - Bytes read: " << total << std::endl;
    Log::info() << "fdb-hammer - Total duration: " << timer.elapsed() << std::endl;
    // Log::info() << "fdb-hammer - Total rate: " << double(total) / timer.elapsed() << " bytes / s" << std::endl;
    // Log::info() << "fdb-hammer - Total rate: " << double(total) / (timer.elapsed() * 1024 * 1024) << " MB / s" << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWrite app(argc, argv);
    return app.start();
}

