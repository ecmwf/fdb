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
#include <chrono>
#include <iomanip>
#include <memory>
#include <random>
#include <thread>
#include <unordered_set>

#include "eccodes.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/StdFile.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageArchiver.h"
#include "fdb5/tools/FDBTool.h"

// This list is currently sufficient to get to nparams=200 of levtype=ml,type=fc
const std::unordered_set<size_t> AWKWARD_PARAMS{11,  12,  13,  14,  15,  16,  49,  51,  52,  61,  121,
                                                122, 146, 147, 169, 175, 176, 177, 179, 189, 201, 202};


using namespace eckit;


class FDBHammer : public fdb5::FDBTool {

    void usage(const std::string& tool) const override;

    void init(const eckit::option::CmdArgs& args) override;

    int minimumPositionalArguments() const override { return 1; }

    void execute(const eckit::option::CmdArgs& args) override;

    void executeRead(const eckit::option::CmdArgs& args);
    void executeWrite(const eckit::option::CmdArgs& args);
    void executeList(const eckit::option::CmdArgs& args);

public:

    FDBHammer(int argc, char** argv) : fdb5::FDBTool(argc, argv), verbose_(false) {

        options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "Reset expver on data"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("class", "Reset class on data"));
        options_.push_back(new eckit::option::SimpleOption<bool>("statistics", "Report statistics after run"));
        options_.push_back(new eckit::option::SimpleOption<bool>("read", "Read rather than write the data"));
        options_.push_back(new eckit::option::SimpleOption<bool>("list", "List rather than write the data"));
        options_.push_back(new eckit::option::SimpleOption<long>("nsteps", "Number of steps"));
        options_.push_back(new eckit::option::SimpleOption<long>("nensembles", "Number of ensemble members"));
        options_.push_back(new eckit::option::SimpleOption<long>("number", "The first ensemble number to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nlevels", "Number of levels"));
        options_.push_back(new eckit::option::SimpleOption<long>("level", "The first level number to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nparams", "Number of parameters"));
        options_.push_back(new eckit::option::SimpleOption<bool>("verbose", "Print verbose output"));
        options_.push_back(new eckit::option::SimpleOption<bool>("disable-subtocs", "Disable use of subtocs"));
        options_.push_back(new eckit::option::SimpleOption<bool>("delay", "Add random delay"));
    }
    ~FDBHammer() override {}

private:

    bool verbose_;
};

void FDBHammer::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool
                       << " [--statistics] [--read] [--list] --nsteps=<nsteps> --nensembles=<nensembles> "
                          "--nlevels=<nlevels> --nparams=<nparams> --expver=<expver> <grib_path>"
                       << std::endl;
    fdb5::FDBTool::usage(tool);
}

void FDBHammer::init(const eckit::option::CmdArgs& args) {
    FDBTool::init(args);

    ASSERT(args.has("expver"));
    ASSERT(args.has("class"));
    ASSERT(args.has("nlevels"));
    ASSERT(args.has("nsteps"));
    ASSERT(args.has("nparams"));

    verbose_ = args.getBool("verbose", false);
}

void FDBHammer::execute(const eckit::option::CmdArgs& args) {

    if (args.getBool("read", false)) {
        executeRead(args);
    }
    else if (args.getBool("list", false)) {
        executeList(args);
    }
    else {
        executeWrite(args);
    }
}

void FDBHammer::executeWrite(const eckit::option::CmdArgs& args) {

    eckit::AutoStdFile fin(args(0));

    int err;
    codes_handle* handle = codes_handle_new_from_file(nullptr, fin, PRODUCT_GRIB, &err);
    ASSERT(handle);

    size_t nsteps     = args.getLong("nsteps");
    size_t nensembles = args.getLong("nensembles", 1);
    size_t nlevels    = args.getLong("nlevels");
    size_t nparams    = args.getLong("nparams");
    size_t number     = args.getLong("number", 1);
    size_t level      = args.getLong("level", 1);

    bool delay = args.getBool("delay", false);


    const char* buffer = nullptr;
    size_t size        = 0;

    eckit::LocalConfiguration userConfig{};
    if (!args.has("disable-subtocs"))
        userConfig.set("useSubToc", true);

    if (delay) {
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dist(0, 10000);

        int delayDuration = dist(mt);
        std::this_thread::sleep_for(std::chrono::milliseconds(delayDuration));
    }

    fdb5::MessageArchiver archiver(fdb5::Key(), false, verbose_, config(args, userConfig));

    std::string expver = args.getString("expver");
    size               = expver.length();
    CODES_CHECK(codes_set_string(handle, "expver", expver.c_str(), &size), 0);
    std::string cls = args.getString("class");
    size            = cls.length();
    CODES_CHECK(codes_set_string(handle, "class", cls.c_str(), &size), 0);

    struct timeval tval_before_io, tval_after_io;
    eckit::Timer timer;
    eckit::Timer gribTimer;
    double elapsed_grib = 0;
    size_t writeCount   = 0;
    size_t bytesWritten = 0;

    timer.start();

    for (size_t member = 1; member <= nensembles; ++member) {
        if (args.has("nensembles")) {
            CODES_CHECK(codes_set_long(handle, "number", member + number - 1), 0);
        }
        for (size_t step = 0; step < nsteps; ++step) {
            CODES_CHECK(codes_set_long(handle, "step", step), 0);
            for (size_t lev = 1; lev <= nlevels; ++lev) {
                CODES_CHECK(codes_set_long(handle, "level", lev + level - 1), 0);
                for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
                    // GRIB API only allows us to use certain parameters
                    while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
                        real_param++;
                    }

                    Log::info() << "Member: " << member << ", step: " << step << ", level: " << level
                                << ", param: " << real_param << std::endl;

                    CODES_CHECK(codes_set_long(handle, "paramId", real_param), 0);

                    CODES_CHECK(codes_get_message(handle, reinterpret_cast<const void**>(&buffer), &size), 0);

                    gribTimer.stop();
                    elapsed_grib += gribTimer.elapsed();

                    MemoryHandle dh(buffer, size);

                    if (member == 1 && step == 0 && lev == 1 && param == 1)
                        gettimeofday(&tval_before_io, NULL);
                    archiver.archive(dh);
                    writeCount++;
                    bytesWritten += size;

                    gribTimer.start();
                }
            }

            gribTimer.stop();
            elapsed_grib += gribTimer.elapsed();
            archiver.flush();
            if (member == nensembles && step == (nsteps - 1))
                gettimeofday(&tval_after_io, NULL);
            gribTimer.start();
        }
    }

    gribTimer.stop();
    elapsed_grib += gribTimer.elapsed();

    timer.stop();

    codes_handle_delete(handle);

    Log::info() << "Fields written: " << writeCount << std::endl;
    Log::info() << "Bytes written: " << bytesWritten << std::endl;
    Log::info() << "Total duration: " << timer.elapsed() << std::endl;
    Log::info() << "GRIB duration: " << elapsed_grib << std::endl;
    Log::info() << "Writing duration: " << timer.elapsed() - elapsed_grib << std::endl;
    Log::info() << "Total rate: " << double(bytesWritten) / timer.elapsed() << " bytes / s" << std::endl;
    Log::info() << "Total rate: " << double(bytesWritten) / (timer.elapsed() * 1024 * 1024) << " MB / s" << std::endl;

    Log::info() << "Timestamp before first IO: " << (long int)tval_before_io.tv_sec << "." << std::setw(6)
                << std::setfill('0') << (long int)tval_before_io.tv_usec << std::endl;
    Log::info() << "Timestamp after last IO: " << (long int)tval_after_io.tv_sec << "." << std::setw(6)
                << std::setfill('0') << (long int)tval_after_io.tv_usec << std::endl;
}


void FDBHammer::executeRead(const eckit::option::CmdArgs& args) {


    fdb5::MessageDecoder decoder;
    std::vector<metkit::mars::MarsRequest> requests = decoder.messageToRequests(args(0));

    ASSERT(requests.size() == 1);
    metkit::mars::MarsRequest request = requests[0];

    size_t nsteps     = args.getLong("nsteps");
    size_t nensembles = args.getLong("nensembles", 1);
    size_t nlevels    = args.getLong("nlevels");
    size_t nparams    = args.getLong("nparams");
    size_t number     = args.getLong("number", 1);
    size_t level      = args.getLong("level", 1);

    //retrieve,class=od,expver=0001,stream=eefo,date=20250610,time=0000,domain=g,type=fcstdev,levtype=sfc,
    std::string numbers = "94/99/89/100/88/92/87/98/81/95/91/84/93/85/86/82/97/90/80/83/96/77/79/78/35/68/64/74/36/28/29/32/40/72/65/66/7/30/16/39/33/38/11/76/37/56/42/45/69/31/25/14/1/12/62/17/6/73/23/9/2/71/50/24/0/21/34/43/18/3/75/15/63/70/13/57/54/61/10/59/47/67/20/5/51/41/8/22/44/19/48/53/55/4/52/49/26/60/46/58/27";
    std::string params = "121/122/136/137/139/141/151/151130/151131/151132/151145/151148/151163/151164/151175/159/164/165/166/167/168/170/172142/172143/172144/172146/172147/172169/172175/172176/172177/172178/172179/172180/172181/172182/172189/172228/201/202/207/228246/228247/235117/31/33/34/39/40/41/42/49/78/79/239117";
    std::string steps = "0-168/120-288/144-312/168-336/192-360/216-384/24-192/240-408/264-432/288-456/312-480/336-504/360-528/384-552/408-576/432-600/456-624/48-216/480-648/504-672/528-696/552-720/576-744/600-768/624-792/648-816/672-840/696-864/72-240/720-888/744-912/768-936/792-960/816-984/840-1008/864-1032/888-1056/912-1080/936-1104/96-264";

    std::vector<std::string> numberlist;
    for (const auto& element : eckit::Tokenizer("/").tokenize(numbers)) {
        numberlist.push_back(element);
    }

    std::vector<std::string> paramlist;
    for (const auto& element : eckit::Tokenizer("/").tokenize(params)) {
        paramlist.push_back(element);
    }

    std::vector<std::string> steplist;
    for (const auto& element : eckit::Tokenizer("/").tokenize(steps)) {
        steplist.push_back(element);
    }

    request.setValue("expver", args.getString("expver"));
    request.setValue("class", args.getString("class"));
    request.setValue("optimised", "on");

    eckit::LocalConfiguration userConfig{};
    if (!args.has("disable-subtocs"))
        userConfig.set("useSubToc", true);

    struct timeval tval_before_io, tval_after_io;
    eckit::Timer timer;
    timer.start();

    fdb5::HandleGatherer handles(false);
    fdb5::FDB fdb(config(args, userConfig));
    size_t fieldsRead = 0;

    gettimeofday(&tval_before_io, NULL);

    for (const auto& number : numberlist) {
        std::cout << "Iterating member " << number << std::endl;
        request.setValue("number", number);
        for (const auto& step : steplist) {
            request.setValue("step", step);
            for (const auto& param : paramlist) {
                request.setValue("param", param);

                handles.add(fdb.retrieve(request));
                fieldsRead++;

            }
        }
    }

    std::unique_ptr<eckit::DataHandle> dh(handles.dataHandle());

    EmptyHandle nullOutputHandle;
    size_t total = 0;
    // size_t total = dh->saveInto(nullOutputHandle);
    gettimeofday(&tval_after_io, NULL);

    timer.stop();

    Log::info() << "Fields read: " << fieldsRead << std::endl;
    Log::info() << "Bytes read: " << total << std::endl;
    Log::info() << "Total duration: " << timer.elapsed() << std::endl;
    Log::info() << "Total rate: " << double(total) / timer.elapsed() << " bytes / s" << std::endl;
    Log::info() << "Total rate: " << double(total) / (timer.elapsed() * 1024 * 1024) << " MB / s" << std::endl;

    Log::info() << "Timestamp before first IO: " << (long int)tval_before_io.tv_sec << "." << std::setw(6)
                << std::setfill('0') << (long int)tval_before_io.tv_usec << std::endl;
    Log::info() << "Timestamp after last IO: " << (long int)tval_after_io.tv_sec << "." << std::setw(6)
                << std::setfill('0') << (long int)tval_after_io.tv_usec << std::endl;
}


void FDBHammer::executeList(const eckit::option::CmdArgs& args) {


    std::vector<std::string> minimumKeys =
        eckit::Resource<std::vector<std::string>>("FDBInspectMinimumKeys", "class,expver", true);

    fdb5::MessageDecoder decoder;
    std::vector<metkit::mars::MarsRequest> requests = decoder.messageToRequests(args(0));

    ASSERT(requests.size() == 1);
    metkit::mars::MarsRequest request = requests[0];

    size_t nsteps     = args.getLong("nsteps");
    size_t nensembles = args.getLong("nensembles", 1);
    size_t nlevels    = args.getLong("nlevels");
    size_t nparams    = args.getLong("nparams");
    size_t number     = args.getLong("number", 1);
    size_t level      = args.getLong("level", 1);

    request.setValue("expver", args.getString("expver"));
    request.setValue("class", args.getString("class"));

    eckit::LocalConfiguration userConfig{};
    if (!args.has("disable-subtocs"))
        userConfig.set("useSubToc", true);

    eckit::Timer timer;
    timer.start();

    fdb5::FDB fdb(config(args, userConfig));
    fdb5::ListElement info;

    std::vector<std::string> number_values;
    for (size_t n = 1; n <= nensembles; ++n) {
        number_values.push_back(std::to_string(n + number - 1));
    }
    request.values("number", number_values);

    std::vector<std::string> levelist_values;
    for (size_t l = 1; l <= nlevels; ++l) {
        levelist_values.push_back(std::to_string(l + level - 1));
    }
    request.values("levelist", levelist_values);

    std::vector<std::string> param_values;
    for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
        // GRIB API only allows us to use certain parameters
        while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
            real_param++;
        }
        param_values.push_back(std::to_string(real_param));
    }
    request.values("param", param_values);

    size_t count = 0;
    for (size_t step = 0; step < nsteps; ++step) {

        request.setValue("step", step);

        auto listObject = fdb.list(fdb5::FDBToolRequest(request, false, minimumKeys));
        while (listObject.next(info)) {
            count++;
        }
    }

    timer.stop();

    Log::info() << "fdb-hammer - Fields listed: " << count << std::endl;
    Log::info() << "fdb-hammer - List duration: " << timer.elapsed() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    FDBHammer app(argc, argv);
    return app.start();
}
