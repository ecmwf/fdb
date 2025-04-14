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
#include <iomanip>
#include <unordered_set>
#include <memory>
#include <iomanip>
#include <algorithm>
#include <random>

#include "eccodes.h"

#include "eckit/config/Resource.h"
#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/StdFile.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/message/MessageArchiver.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/tools/FDBTool.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include <unistd.h>
#include <limits.h>
#include "eckit/log/TimeStamp.h"
#include "eckit/utils/MD5.h"
#include "eckit/message/Reader.h"
#include "eckit/message/Message.h"

// This list is currently sufficient to get to nparams=200 of levtype=ml,type=fc
const std::unordered_set<size_t> AWKWARD_PARAMS {11, 12, 13, 14, 15, 16, 49, 51, 52, 61, 121, 122, 146, 147, 169, 175, 176, 177, 179, 189, 201, 202};


using namespace eckit;


class FDBHammer : public fdb5::FDBTool {

    void usage(const std::string &tool) const override;

    void init(const eckit::option::CmdArgs &args) override;

    int minimumPositionalArguments() const override { return 1; }

    void execute(const eckit::option::CmdArgs &args) override;

    void executeRead(const eckit::option::CmdArgs& args);
    void executeWrite(const eckit::option::CmdArgs& args);
    void executeList(const eckit::option::CmdArgs& args);

public:

    FDBHammer(int argc, char **argv) :
        fdb5::FDBTool(argc, argv),
        verbose_(false), md_check_(false), full_check_(false) {

        options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "Reset expver on data"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("class", "Reset class on data"));
        options_.push_back(new eckit::option::SimpleOption<bool>("statistics", "Report statistics after run"));
        options_.push_back(new eckit::option::SimpleOption<bool>("read", "Read rather than write the data"));
        options_.push_back(new eckit::option::SimpleOption<bool>("list", "List rather than write the data"));
        options_.push_back(new eckit::option::SimpleOption<bool>("itt", "Run the benchmark in ITT mode"));
        options_.push_back(new eckit::option::SimpleOption<long>("poll-period", 
             "Number of seconds to use as polling period for readers in ITT mode"));
        options_.push_back(new eckit::option::SimpleOption<long>("nsteps", "Number of steps"));
        options_.push_back(new eckit::option::SimpleOption<long>("step", "The first step number to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nensembles", "Number of ensemble members"));
        options_.push_back(new eckit::option::SimpleOption<long>("number", "The first ensemble number to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nlevels", "Number of levels"));
        options_.push_back(new eckit::option::SimpleOption<long>("level", "The first level number to use"));
        options_.push_back(new eckit::option::SimpleOption<string>("levels", "Comma-separated list of level numbers to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nparams", "Number of parameters"));
        options_.push_back(new eckit::option::SimpleOption<bool>("verbose", "Print verbose output"));
        options_.push_back(new eckit::option::SimpleOption<bool>("disable-subtocs", "Disable use of subtocs"));
        options_.push_back(new eckit::option::SimpleOption<bool>("md-check", 
            "Calculate a metadata checksum (checksum of the field key + unique operation identifier) on every field write and "
           "insert it at the begginning and end of the encoded message data payload. "
           "On every field read, the checksums are extracted from the message data payload, the checksum of the FDB field key "
           "is recalculated, and all checksums are checked to match."));
        options_.push_back(new eckit::option::SimpleOption<bool>("full-check", 
            "Insert a metadata checksum (checksum of the field key + unique operation identifier) and a checksum of the full "
           "field data payload, on every field write, at the begginning of the encoded message data payload. The data checksum "
           "includes the unique operation identifier in its calculation, and the resulting checksum is written at the beginning "
           "of the encoded message data payload between the field key checksum and the operation identifier. "
           "On every field read, the key checksum is extracted from the message data payload, the key checksum is recalculated "
           "from FDB field key, and both checksums are compared. The checksum of the data payload is also extracted and "
           "recalculated from the message data payload (excluding the key checksum but including the operation identifier), "
           "and both checksums are checked to match."));
    }
    ~FDBHammer() override {}

private:
    bool verbose_;
    bool itt_;
    bool md_check_;
    bool full_check_;
};

void FDBHammer::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool
                       << " [--read] [--list] [--itt] [--poll-period=<period>] [--md-check|--full-check] [--statistics] "
                          "--nsteps=<nsteps> [--step=<step>] "
                          "--nensembles=<nensembles> [--number=<number>] "
                          "--nlevels=<nlevels> [--level=<level>|--levels=<level1,level2,...>] "
                          "--nparams=<nparams> --expver=<expver> <grib_path>"
                       << std::endl;
    fdb5::FDBTool::usage(tool);
}

void FDBHammer::init(const eckit::option::CmdArgs& args)
{
    FDBTool::init(args);

    ASSERT(args.has("expver"));
    ASSERT(args.has("class"));
    ASSERT(args.has("nlevels"));
    ASSERT(args.has("nsteps"));
    ASSERT(args.has("nparams"));

    verbose_ = args.getBool("verbose", false);

    itt_ = args.getBool("itt", false);

    md_check_ = args.getBool("md-check", false);
    full_check_ = args.getBool("full-check", false);
    if (full_check_) md_check_ = false;
}

void FDBHammer::execute(const eckit::option::CmdArgs &args) {

    if (args.getBool("read", false)) {
        executeRead(args);
    } else if (args.getBool("list", false)) {
        executeList(args);
    } else {
        executeWrite(args);
    }
}

void FDBHammer::executeWrite(const eckit::option::CmdArgs &args) {

    eckit::AutoStdFile fin(args(0));

    int err;
    codes_handle* handle = codes_handle_new_from_file(nullptr, fin, PRODUCT_GRIB, &err);
    ASSERT(handle);

    size_t nsteps      = args.getLong("nsteps");
    size_t step        = args.getLong("step", 0);
    size_t nensembles  = args.getLong("nensembles", 1);
    size_t number      = args.getLong("number", 1);
    size_t nlevels     = args.getLong("nlevels");
    size_t level       = args.getLong("level", 1);
    std::string levels = args.getLong("levels", "");
    size_t nparams     = args.getLong("nparams");

    const char* buffer = nullptr;
    size_t size = 0;

    eckit::LocalConfiguration userConfig{};
    if (!args.has("disable-subtocs")) userConfig.set("useSubToc", true);

    std::vector<size_t> levelist;
    if (args.has("levels")) {
        for (const auto& element : eckit::Tokenizer(",").tokenize(levels)) {
            levelist.push_back(eckit::Translator<std::string, size_t>()(element));
        }
    } else {
        for (size_t ilevel = level; ilevel <= nlevels + level; ++ilevel) {
            levelist.push_back(ilevel);
        }
    }

    fdb5::MessageArchiver archiver(fdb5::Key(), false, verbose_, config(args, userConfig));

    std::string expver = args.getString("expver");
    size = expver.length();
    CODES_CHECK(codes_set_string(handle, "expver", expver.c_str(), &size), 0);
    std::string cls = args.getString("class");
    size = cls.length();
    CODES_CHECK(codes_set_string(handle, "class", cls.c_str(), &size), 0);

    eckit::Translator<size_t,std::string> str;

    struct timeval tval_before_io, tval_after_io;
    eckit::Timer timer;
    eckit::Timer gribTimer;
    double elapsed_grib = 0;
    size_t writeCount = 0;
    size_t bytesWritten = 0;

    timer.start();

    for (size_t member = number; member <= nensembles + number; ++member) {
        if (args.has("nensembles")) {
            CODES_CHECK(codes_set_long(handle, "number", member), 0);
        }
        for (size_t istep = step; istep < nsteps + step; ++istep) {
            CODES_CHECK(codes_set_long(handle, "step", istep), 0);
            for (const auto& ilevel : levelist) {
                CODES_CHECK(codes_set_long(handle, "level", ilevel), 0);
                for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
                    // GRIB API only allows us to use certain parameters
                    while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
                        real_param++;
                    }

                    Log::info() << "Member: " << member << ", step: " << istep << ", level: " << ilevel
                                << ", param: " << real_param << std::endl;

                    CODES_CHECK(codes_set_long(handle, "paramId", real_param), 0);

                    CODES_CHECK(codes_get_message(handle, reinterpret_cast<const void**>(&buffer), &size), 0);

                    if (full_check_ or md_check_) {

                        // get message data offset
                        long offsetBeforeData = 0, offsetAfterData = 0;
                        CODES_CHECK(codes_get_long(handle, std::string("offsetBeforeData").c_str(), &offsetBeforeData), 0);
                        CODES_CHECK(codes_get_long(handle, std::string("offsetAfterData").c_str(), &offsetAfterData), 0);

                        // generate a checksum of the FDB key
                        fdb5::Key key({
                            {"number", str(member+number-1)},
                            {"step", str(step)},
                            {"level", str(lev+level-1)},
                            {"param", str(real_param)},
                        });
                        std::string key_string(key);
                        eckit::MD5 md5(key_string);
                        std::string digest = md5.digest();
                        uint64_t key_hi = std::stoull(digest.substr(0, 8), nullptr, 16);
                        uint64_t key_lo = std::stoull(digest.substr(8, 16), nullptr, 16);

                        // generate a unique write operation ID and calculate its checksum
                        /// @note: copied from LocalPathName::unique. Ditched StaticMutex - not thread safe
                        std::string hostname = eckit::Main::hostname();
                        static unsigned long long n = (((unsigned long long)::getpid()) << 32);
                        static std::string format = "%Y%m%d.%H%M%S";
                        std::ostringstream os;
                        os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
                        std::string uid = os.str();
                        while (::access(uid.c_str(), F_OK) == 0) {
                            std::ostringstream os;
                            os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
                            uid = os.str();
                        }
                        md5.reset();
                        md5.add(uid);
                        digest = md5.digest();
                        uint64_t uid_hi = std::stoull(digest.substr(0, 8), nullptr, 16);
                        uint64_t uid_lo = std::stoull(digest.substr(8, 16), nullptr, 16);

                        if (md_check_) {

                            // write checksums in message data region
                            // message = grib header | fdb key checksum | unique id checksum | grib data | fdb key checksum | unique id checksum
                            ::memcpy(&const_cast<char*>(buffer)[offsetBeforeData], &key_hi, sizeof(key_hi));
                            ::memcpy(&const_cast<char*>(buffer)[offsetBeforeData + sizeof(key_hi)], &key_lo, sizeof(key_lo));
                            ::memcpy(&const_cast<char*>(buffer)[offsetBeforeData + 2 * sizeof(key_lo)], &uid_hi, sizeof(uid_hi));
                            ::memcpy(&const_cast<char*>(buffer)[offsetBeforeData + 3 * sizeof(key_lo)], &uid_lo, sizeof(uid_lo));
                            ::memcpy(&const_cast<char*>(buffer)[offsetAfterData - 4 * sizeof(key_lo)], &key_hi, sizeof(key_hi));
                            ::memcpy(&const_cast<char*>(buffer)[offsetAfterData - 3 * sizeof(key_lo)], &key_lo, sizeof(key_lo));
                            ::memcpy(&const_cast<char*>(buffer)[offsetAfterData - 2 * sizeof(key_lo)], &uid_hi, sizeof(uid_hi));
                            ::memcpy(&const_cast<char*>(buffer)[offsetAfterData - 1 * sizeof(key_lo)], &uid_lo, sizeof(uid_lo));

                        }

                        if (full_check_) {

                            // checksums will be laid out as follows:
                            // message = grib header | fdb key checksum | data checksum | unique id checksum | grib data

                            long uidChecksumOffset = offsetBeforeData + 4 * sizeof(key_lo);

                            // write operation uid checksum
                            ::memcpy(&const_cast<char*>(buffer)[uidChecksumOffset], &uid_hi, sizeof(uid_hi));
                            ::memcpy(&const_cast<char*>(buffer)[uidChecksumOffset + sizeof(key_lo)], &uid_lo, sizeof(uid_lo));

                            // calculate cheksum of data excluding the key and data checksum ranges
                            md5.reset();
                            md5.add(buffer + uidChecksumOffset, offsetAfterData - uidChecksumOffset);
                            digest = md5.digest();
                            uint64_t data_hi = std::stoull(digest.substr(0, 8), nullptr, 16);
                            uint64_t data_lo = std::stoull(digest.substr(8, 16), nullptr, 16);

                            // write checksums in message data region
                            ::memcpy(&const_cast<char*>(buffer)[offsetBeforeData], &key_hi, sizeof(key_hi));
                            ::memcpy(&const_cast<char*>(buffer)[offsetBeforeData + sizeof(key_hi)], &key_lo, sizeof(key_lo));
                            ::memcpy(&const_cast<char*>(buffer)[offsetBeforeData + 2 * sizeof(key_lo)], &data_hi, sizeof(data_hi));
                            ::memcpy(&const_cast<char*>(buffer)[offsetBeforeData + 3 * sizeof(key_lo)], &data_lo, sizeof(data_lo));

                        }
                    }

                    gribTimer.stop();
                    elapsed_grib += gribTimer.elapsed();

                    MemoryHandle dh(buffer, size);

                    if (member == number && istep == step && ilevel == level && param == 1)
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
            if (member == nensembles && step == (nsteps - 1)) gettimeofday(&tval_after_io, NULL);
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

    Log::info() << "Timestamp before first IO: " <<
                    (long int)tval_before_io.tv_sec << "." <<
                    std::setw(6) << std::setfill('0') <<
                    (long int)tval_before_io.tv_usec << std::endl;
    Log::info() << "Timestamp after last IO: " <<
                    (long int)tval_after_io.tv_sec << "." <<
                    std::setw(6) << std::setfill('0') <<
                    (long int)tval_after_io.tv_usec << std::endl;

}


void FDBHammer::executeRead(const eckit::option::CmdArgs &args) {


    fdb5::MessageDecoder decoder;
    std::vector<metkit::mars::MarsRequest> requests = decoder.messageToRequests(args(0));

    ASSERT(requests.size() == 1);
    metkit::mars::MarsRequest request = requests[0];

    size_t nsteps      = args.getLong("nsteps");
    size_t step        = args.getLong("step", 0);
    size_t nensembles  = args.getLong("nensembles", 1);
    size_t number      = args.getLong("number", 1);
    size_t nlevels     = args.getLong("nlevels");
    size_t level       = args.getLong("level", 1);
    std::string levels = args.getLong("levels", "");
    size_t nparams     = args.getLong("nparams");
    size_t poll_period = args.getLong("poll-period", 1);

    request.setValue("expver", args.getString("expver"));
    request.setValue("class", args.getString("class"));
    request.setValue("optimised", "on");

    eckit::LocalConfiguration userConfig{};
    if (!args.has("disable-subtocs")) userConfig.set("useSubToc", true);

    std::vector<size_t> levelist;
    if (args.has("levels")) {
        for (const auto& element : eckit::Tokenizer(",").tokenize(levels) {
            levelist.push_back(eckit::Translator<std::string, size_t>()(element);
        }
    } else {
        for (size_t ilevel = level; ilevel <= nlevels + level; ++ilevel) {
            levelist.push_back(ilevel);
        }
    }

    std::vector<size_t> paramlist;
    for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
        // GRIB API only allows us to use certain parameters
        while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
            real_param++;
        }
        paramlist.push_back(real_param);
    }
    if (itt_) {
        auto rng = std::default_random_engine {};
        std::shuffle(std::begin(paramlist), std::end(paramlist), rng);
    }

    struct timeval tval_before_io, tval_after_io;
    eckit::Timer timer;
    timer.start();

    fdb5::HandleGatherer handles(false);
    std::optional<fdb5::FDB> fdb;
    fdb5::FDBToolRequest list_request{request, false};
    size_t fieldsRead = 0;

    for (size_t member = number; member <= nensembles + number; ++member) {
        if (args.has("nensembles")) {
            request.setValue("number", member);
        }
        for (size_t istep = step; istep < nsteps; ++istep) {
            request.setValue("step", step);
            for (const auto& ilevel : levelist) {
                request.setValue("levelist", ilevel);
                for (const auto& param : paramlist) {
                    request.setValue("param", param);

                    Log::info() << "Member: " << member << ", step: " << istep << ", level: " << ilevel
                                << ", param: " << real_param << std::endl;

                    if (member == number && istep == step && ilevel == level && param == 1) {
                        if (itt_) {
                            bool dataReady = false;
                            while (!dataReady) {
                                fdb.emplace(config(args, userConfig));
                                auto listObject = fdb->list(list_request);
                                size_t count = 0;
                                fdb5::ListElement info;
                                while (listObject.next(info)) ++count;
                                if (count > 0) {
                                    dataReady = true;
                                } else {
                                    sleep(poll_period);
                                }
                            }
                        }
                        gettimeofday(&tval_before_io, NULL);
                    }
                    handles.add(fdb->retrieve(request));
                    fieldsRead++;
                }
            }
        }
    }

    std::unique_ptr<eckit::DataHandle> dh(handles.dataHandle());

    eckit::Optional<eckit::Buffer> buff;
    eckit::Optional<eckit::MemoryHandle> mh;
    eckit::Optional<eckit::Length> original_size;

    size_t total = 0;

    if (full_check_ || md_check_) {
        // if storing all read data in memory for later checksum calculation and verification
        // is not an option, it could be stored and processed by parts as follows:
        //
        // struct Hack {
        //   bool sorted_;
        //   std::vector<eckit::DataHandle *> handles_;
        //   size_t count_;
        // }
        // Hack* dh2 = reinterpret_cast<Hack *>(dh.get());
        // for (auto& ph : dh2->datahandles_) { ... }

        eckit::FileHandle fin(args(0));
        original_size.emplace(fin.size());
        size_t expected = nensembles * nsteps * nlevels * nparams * (size_t)original_size.value();
        buff.emplace(expected);
        mh.emplace(buff.value(), expected);
        total = dh->saveInto(mh.value());
    } else {
        EmptyHandle nullOutputHandle;
        total = dh->saveInto(nullOutputHandle);
    }

    gettimeofday(&tval_after_io, NULL);

    timer.stop();

    if (full_check_ || md_check_) {

        eckit::Translator<size_t,std::string> str;

        eckit::message::Reader reader(mh.value());
        eckit::message::Message msg;

        for (size_t member = 1; member <= nensembles; ++member) {
            for (size_t step = 0; step < nsteps; ++step) {
                for (size_t lev = 1; lev <= nlevels; ++lev) {
                    for (size_t param = 1, real_param = 1; param <= nparams; ++param, ++real_param) {
                        // GRIB API only allows us to use certain parameters
                        while (AWKWARD_PARAMS.find(real_param) != AWKWARD_PARAMS.end()) {
                            real_param++;
                        }

                        if (!(msg = reader.next())) throw eckit::Exception("Found less fields than expected.");

                        if (eckit::Length(msg.length()) != original_size.value()) throw eckit::Exception("Found a field of different size than the seed.");

                        // TODO: get full grib key to ensure grib header is not corrupted
                        //fdb5::Key keycheck;
                        //fdb5::KeySetter setter(keycheck);
                        //msg.getMetadata(setter);

                        // get message data offset
                        long offsetBeforeData = msg.getLong("offsetBeforeData");
                        long offsetAfterData = msg.getLong("offsetAfterData");

                        // generate a checksum of the FDB key
                        fdb5::Key key({
                            {"number", str(member+number-1)},
                            {"step", str(step)},
                            {"level", str(lev+level-1)},
                            {"param", str(real_param)},
                        });
                        std::string key_string(key);
                        eckit::MD5 md5(key_string);
                        std::string digest = md5.digest();
                        uint64_t key_hi = std::stoull(digest.substr(0, 8), nullptr, 16);
                        uint64_t key_lo = std::stoull(digest.substr(8, 16), nullptr, 16);

                        if (md_check_) {

                            long key_hi1 = 0, key_lo1 = 0, key_hi2 = 0, key_lo2 = 0;
                            long uid_hi1 = 0, uid_lo1 = 0, uid_hi2 = 0, uid_lo2 = 0;
                            ::memcpy(&key_hi1, &((char*)(msg.data()))[offsetBeforeData], sizeof(key_hi));
                            ::memcpy(&key_lo1, &((char*)(msg.data()))[offsetBeforeData + sizeof(key_hi)], sizeof(key_hi));
                            ::memcpy(&uid_hi1, &((char*)(msg.data()))[offsetBeforeData + 2 * sizeof(key_hi)], sizeof(key_hi));
                            ::memcpy(&uid_lo1, &((char*)(msg.data()))[offsetBeforeData + 3 * sizeof(key_hi)], sizeof(key_hi));
                            ::memcpy(&key_hi2, &((char*)(msg.data()))[offsetAfterData - 4 * sizeof(key_lo)], sizeof(key_hi));
                            ::memcpy(&key_lo2, &((char*)(msg.data()))[offsetAfterData - 3 * sizeof(key_lo)], sizeof(key_hi));
                            ::memcpy(&uid_hi2, &((char*)(msg.data()))[offsetAfterData - 2 * sizeof(key_lo)], sizeof(key_hi));
                            ::memcpy(&uid_lo2, &((char*)(msg.data()))[offsetAfterData - 1 * sizeof(key_lo)], sizeof(key_hi));

                            ASSERT(key_hi == key_hi1 && key_hi == key_hi2);
                            ASSERT(key_lo == key_lo1 && key_lo == key_lo2);
                            ASSERT(uid_hi1 == uid_hi2);
                            ASSERT(uid_lo1 == uid_lo2);

                        }

                        if (full_check_) {

                            // calculate cheksum of data excluding the key and data checksum ranges
                            long uidChecksumOffset = offsetBeforeData + 4 * sizeof(key_lo);
                            eckit::MD5 md5(&((char*)(msg.data()))[uidChecksumOffset], offsetAfterData - uidChecksumOffset);
                            digest = md5.digest();
                            uint64_t data_hi = std::stoull(digest.substr(0, 8), nullptr, 16);
                            uint64_t data_lo = std::stoull(digest.substr(8, 16), nullptr, 16);

                            long key_hi1 = 0, key_lo1 = 0, data_hi1 = 0, data_lo1 = 0;
                            ::memcpy(&key_hi1, &((char*)(msg.data()))[offsetBeforeData], sizeof(key_hi));
                            ::memcpy(&key_lo1, &((char*)(msg.data()))[offsetBeforeData + sizeof(key_hi)], sizeof(key_hi));
                            ::memcpy(&data_hi1, &((char*)(msg.data()))[offsetBeforeData + 2 * sizeof(key_hi)], sizeof(key_hi));
                            ::memcpy(&data_lo1, &((char*)(msg.data()))[offsetBeforeData + 3 * sizeof(key_hi)], sizeof(key_hi));

                            ASSERT(key_hi == key_hi1);
                            ASSERT(key_lo == key_lo1);
                            ASSERT(data_hi == data_hi1);
                            ASSERT(data_lo == data_lo1);

                        }

                    }
                }
            }
        }

    }

    Log::info() << "Fields read: " << fieldsRead << std::endl;
    Log::info() << "Bytes read: " << total << std::endl;
    Log::info() << "Total duration: " << timer.elapsed() << std::endl;
    Log::info() << "Total rate: " << double(total) / timer.elapsed() << " bytes / s" << std::endl;
    Log::info() << "Total rate: " << double(total) / (timer.elapsed() * 1024 * 1024) << " MB / s" << std::endl;

    Log::info() << "Timestamp before first IO: " <<
                    (long int)tval_before_io.tv_sec << "." <<
                    std::setw(6) << std::setfill('0') <<
                    (long int)tval_before_io.tv_usec << std::endl;
    Log::info() << "Timestamp after last IO: " <<
                    (long int)tval_after_io.tv_sec << "." <<
                    std::setw(6) << std::setfill('0') <<
                    (long int)tval_after_io.tv_usec << std::endl;

}


void FDBHammer::executeList(const eckit::option::CmdArgs &args) {


    std::vector<std::string> minimumKeys = eckit::Resource<std::vector<std::string>>("FDBInspectMinimumKeys", "class,expver", true);

    fdb5::MessageDecoder decoder;
    std::vector<metkit::mars::MarsRequest> requests = decoder.messageToRequests(args(0));

    ASSERT(requests.size() == 1);
    metkit::mars::MarsRequest request = requests[0];

    size_t nsteps = args.getLong("nsteps");
    size_t nensembles = args.getLong("nensembles", 1);
    size_t nlevels = args.getLong("nlevels");
    size_t nparams = args.getLong("nparams");
    size_t number = args.getLong("number", 1);
    size_t level = args.getLong("level", 1);

    request.setValue("expver", args.getString("expver"));
    request.setValue("class", args.getString("class"));

    eckit::LocalConfiguration userConfig{};
    if (!args.has("disable-subtocs")) userConfig.set("useSubToc", true);

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

int main(int argc, char **argv) {
    FDBHammer app(argc, argv);
    return app.start();
}

