/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageArchiver.h"
#include "fdb5/tools/FDBTool.h"

#include "metkit/mars/TypeAny.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/io/FileDescHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/StdFile.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/utils/Literals.h"
#include "eckit/utils/MD5.h"

#include "eccodes.h"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <memory>
#include <random>
#include <thread>

#include <fcntl.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>


using namespace eckit;
using namespace eckit::literals;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

namespace {

// Valid type=pf,levtype=pl parameters. This allows for up to 174 parameters.
// Note that not all paramids are valid for encoding GRIB data with all combinations of type, levtype.
// This provides a set which can be used for providing a range corresponding to --nparams.

enum class CheckType : long {
    NONE = 0,
    MD_CHECK,
    FULL_CHECK
};

const std::vector<size_t> VALID_PARAMS{
    1,   2,   3,   4,   5,   6,   7,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  21,  22,  23,  24,  25,
    26,  27,  28,  29,  30,  32,  33,  35,  36,  37,  38,  39,  40,  41,  42,  43,  46,  53,  54,  60,  62,  63,
    64,  65,  66,  67,  70,  71,  74,  75,  76,  77,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,
    92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113,
    114, 115, 116, 117, 118, 119, 120, 123, 125, 126, 127, 128, 129, 130, 131, 132, 133, 135, 138, 139, 140, 141,
    145, 148, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 170, 171, 174, 183, 184, 185, 186, 187, 188,
    190, 191, 192, 193, 194, 197, 198, 199, 200, 203, 204, 205, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
    224, 225, 226, 227, 233, 236, 237, 238, 241, 242, 243, 246, 247, 248, 249, 250, 251, 252, 253, 254};

std::vector<std::string> to_stringvec(const std::vector<long>& vals) {
    std::vector<std::string> result;
    result.reserve(vals.size());
    for (long v : vals) {
        result.emplace_back(std::to_string(v));
    }
    return result;
}

// Extend std::to_string to be able to be templated alongside std::string...

template <typename T>
auto to_string(const T& val) {
    return std::to_string(val);
}

template <>
auto to_string<>(const std::string& val) {
    return val;
}

template <typename Q, typename R, typename S, typename T>
Key shortFDBKey(const Q& step, const R& member, const S& level, const T& param) {
    return {{"number", to_string(member)},
            {"step", to_string(step)},
            {"level", to_string(level)},
            {"param", to_string(param)}};
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

struct HammerConfig {

    // Types

    enum class Mode {
        Write,
        Read,
        List
    };

    struct RequestConfig {
        std::string expver;
        std::string class_;
        std::vector<long> levelist;  // strings --> avoid awkward repeated translation elsewhere
        std::vector<long> paramlist;
        std::vector<long> ensemblelist;
        std::vector<long> steplist;
    };

    /// TODO(ssmart): Some of these should be size_t (unsigned), but that doesn't play well with args.getLong...`

    struct IterationConfig {
        long startAtChunk;
        long stopAtChunk;
        long stepWindow;
        long randomDelay;
        bool hasEnsembles;  // If ensemble is not specified, we shouldn't try and set it on the GRIB
    };

    struct ExecutionConfig {
        Mode mode = Mode::Write;
        PathName templateGrib;
        bool mdCheck;
        bool fullCheck;
        bool randomiseData;
        bool itt;
        std::string uriFile;
        long checkQueueSize;
    };

    struct TimingConfig {
        bool startupDelay;
        long pollPeriod;
        long pollMaxAttempts;
    };

    struct InterfaceConfig {
        bool verbose;
    };

    struct ParallelConfig {
        long ppn;
        std::vector<std::string> nodelist;
        int barrierPort;
        int barrierMaxWait;
    };

    static void constructOptions(std::vector<eckit::option::Option*>& options);
    static HammerConfig parse(const eckit::option::CmdArgs& args);

    static std::vector<long> parseLevelist(const eckit::option::CmdArgs& args);
    static std::vector<long> parseParamlist(const eckit::option::CmdArgs& args);
    static std::vector<long> parseEnsemblelist(const eckit::option::CmdArgs& args);
    static std::vector<long> parseSteplist(const eckit::option::CmdArgs& args);
    static Mode parseMode(const eckit::option::CmdArgs& args);

    metkit::mars::MarsRequest templateRequest() const;

    RequestConfig request;
    IterationConfig iteration;
    ExecutionConfig execution;
    TimingConfig timing;
    InterfaceConfig interface;
    ParallelConfig parallel;
    Config fdbConfig;
};

//----------------------------------------------------------------------------------------------------------------------

class HammerVerifier {

    bool mdCheck_;
    bool fullCheck_;
    std::string hostname_;
    unsigned long long unique_;

    // Used for verification only
    std::deque<Key> verificationList_;
    std::optional<Queue<message::Message>> messageQueue_;
    std::optional<std::thread> verificationWorker_;
    eckit::Length originalSize_;

    // Standardise the data which can be stored
    // TODO(ssmart): Not clear to me why we convert these to longs, rather than just storing the strings?

    class StoredDigest {
        long hi_;
        long lo_;

        StoredDigest(long hi, long lo) : hi_(hi), lo_(lo) {}

    public:

        explicit StoredDigest(const MD5& md5) {
            auto digest = md5.digest();
            hi_ = std::stoull(digest.substr(0, 8), nullptr, 16);
            lo_ = std::stoull(digest.substr(8, 16), nullptr, 16);
        }

        bool operator==(const StoredDigest& other) const { return hi_ == other.hi_ && lo_ == other.lo_; }

        void store(char* data) const {
            auto p = reinterpret_cast<long*>(data);
            p[0] = hi_;
            p[1] = lo_;
        }

        static StoredDigest load(const char* data) {
            auto p = reinterpret_cast<const long*>(data);
            return {p[0], p[1]};
        }
    };

    // We have two possible verification data layouts:
    //
    // 1. MD Check:
    //
    //     - message header
    //     - CheckType MD_CHECK
    //     - FDB key digest
    //     - unique operation id digest
    //     - (potentially random) GRIB data
    //     - FDB key digest
    //     - unique operation id digest
    //
    //    On verifier, the FDB key digests can be constructed, and checked. We can verify that the two unique ID
    //    digests are equal, but their contents are unknown.
    //
    // 2. Full Check:
    //
    //     - message header
    //     - CheckType FULL_CHECK
    //     - FDB key digest
    //     - checksum digest
    //     - unique operation id digest
    //     - non-random GRIB data (from template. Same for all fields)
    //
    //   The checksum digest is calculated on the unique-operation id digest + residual GRIB data. Thus in the
    //   verification step, this data can be read in and the checksub recalculated for verification.

public:

    HammerVerifier(const HammerConfig& config, std::deque<Key>&& verificationList) : HammerVerifier(config) {
        verificationList_ = std::move(verificationList);
        messageQueue_.emplace(config.execution.checkQueueSize);

        // Get the size of the template grib for comparison purposes
        // If we are randomising data, the encoded size will vary depending if CCSDS is enabled, so the
        // size check is not valid.
        if (!config.execution.randomiseData) {
            eckit::FileHandle fin(config.execution.templateGrib);
            originalSize_ = fin.size();
            Log::info() << "Using template GRIB of size " << originalSize_ << std::endl;
        }
    }

    HammerVerifier(const HammerConfig& config) :
        mdCheck_(config.execution.mdCheck),
        fullCheck_(config.execution.fullCheck),
        hostname_(eckit::Main::hostname()),
        unique_((((unsigned long long)::getpid()) << 32)),
        originalSize_(0) {
        ASSERT(!(mdCheck_ && fullCheck_));
    }

    void storeVerificationData(long step, long member, long level, long param, char* data, size_t dataSize) {
        auto key_digest = constructKeyDigest(step, member, level, param);
        auto unique_digest = constructUniqueDigest();
        if (mdCheck_) {
            storeMDVerificationData(key_digest, unique_digest, data, dataSize);
        }
        else if (fullCheck_) {
            storeFullVerificationData(key_digest, unique_digest, data, dataSize);
        }
    }

    void verifyMessage(message::Message&& msg) {

        if (originalSize_ != 0 && eckit::Length(msg.length()) != originalSize_) {
            eckit::Log::info() << "Mismatched size: " << msg.length() << " - " << originalSize_ << std::endl;
            throw eckit::Exception("Found a field of different size than the template GRIB", Here());
        }

        if (mdCheck_ || fullCheck_) {

            if (!verificationWorker_) {
                verificationWorker_.emplace(std::thread([this]() { verifyMessageLoop(); }));
            }

            messageQueue_->emplace(std::move(msg));
        }
    }

    void readComplete() {
        messageQueue_->close();
        verificationWorker_->join();
        ASSERT(messageQueue_->empty());

        if (!verificationList_.empty()) {
            throw Exception("Found fewer fields than expected", Here());
        }
    }

private:

    void verifyMessageLoop() {
        try {
            message::Message msg;
            long n;
            while ((n = messageQueue_->pop(msg)) >= 0) {

                Log::debug() << "queue len: " << n << std::endl;
                if (verificationList_.empty()) {
                    throw Exception("Unexpected field found in retrieved data", Here());
                }

                auto key = verificationList_.front();
                verificationList_.pop_front();
                verifyMessageBlocking(msg, key);
            }
        }
        catch (...) {
            messageQueue_->interrupt(std::current_exception());
            Log::error() << "Aborting (failed) worker thread" << std::endl;
        }
    }

    void verifyMessageBlocking(const message::Message& msg, const fdb5::Key& key) {

        auto key_digest = StoredDigest(MD5(std::string(key)));

        long offsetBeforeData = msg.getLong("offsetBeforeData");
        long offsetAfterData = msg.getLong("offsetAfterData");

        const char* data = &static_cast<const char*>(msg.data())[offsetBeforeData];
        size_t dataSize = offsetAfterData - offsetBeforeData;

        if (mdCheck_) {
            verifyMDVerificationData(key_digest, data, dataSize, key.empty());
        }
        else if (fullCheck_) {
            verifyFullVerificationData(key_digest, data, dataSize, key.empty());
        }
    }

    StoredDigest constructKeyDigest(long step, long member, long level, long param) {

        fdb5::Key key = shortFDBKey(step, member, level, param);
        std::string keystr(key);
        return StoredDigest{MD5(keystr)};
    }

    StoredDigest constructUniqueDigest() {
        std::ostringstream os;
        os << eckit::TimeStamp("%Y%m%d.%H%M%S") << '.' << hostname_ << '.' << unique_++;
        return StoredDigest{MD5(os.str())};
    }


    void storeMDVerificationData(const StoredDigest& key_digest, const StoredDigest& unique_digest, char* data,
                                 size_t dataSize) {

        ASSERT(dataSize > (sizeof(CheckType) + 4 * sizeof(StoredDigest)));
        auto p = reinterpret_cast<long*>(data);
        p[0] = static_cast<long>(CheckType::MD_CHECK);
        key_digest.store(data + sizeof(CheckType));
        unique_digest.store(data + sizeof(CheckType) + sizeof(StoredDigest));
        key_digest.store(data + dataSize - 2 * sizeof(StoredDigest));
        unique_digest.store(data + dataSize - sizeof(StoredDigest));
    }

    void storeFullVerificationData(const StoredDigest& key_digest, const StoredDigest& unique_digest, char* data,
                                   size_t dataSize) {

        ASSERT(dataSize > (sizeof(CheckType) + 3 * sizeof(StoredDigest)));

        auto p = reinterpret_cast<long*>(data);
        p[0] = static_cast<long>(CheckType::FULL_CHECK);
        key_digest.store(data + sizeof(CheckType));

        // Construct a checksum over data including the unique digest

        unique_digest.store(data + sizeof(CheckType) + 2 * sizeof(StoredDigest));

        MD5 md5;
        md5.add(data + sizeof(CheckType) + 2 * sizeof(StoredDigest),
                dataSize - sizeof(CheckType) - 2 * sizeof(StoredDigest));
        StoredDigest checksum_digest(md5);

        checksum_digest.store(data + sizeof(CheckType) + sizeof(StoredDigest));
    }

    void verifyMDVerificationData(const StoredDigest& key_digest, const char* data, size_t dataSize, bool noKey) {

        ASSERT(dataSize > (sizeof(CheckType) + 4 * sizeof(StoredDigest)));
        auto p = reinterpret_cast<const long*>(data);
        ASSERT(p[0] == static_cast<long>(CheckType::MD_CHECK));

        auto key_digest1 = StoredDigest::load(data + sizeof(CheckType));
        auto unique_digest1 = StoredDigest::load(data + sizeof(CheckType) + sizeof(StoredDigest));

        auto key_digest2 = StoredDigest::load(data + dataSize - 2 * sizeof(StoredDigest));
        auto unique_digest2 = StoredDigest::load(data + dataSize - sizeof(StoredDigest));

        if (!noKey) {
            ASSERT(key_digest == key_digest1);
            ASSERT(key_digest == key_digest2);
        }
        ASSERT(key_digest1 == key_digest2);
        ASSERT(unique_digest1 == unique_digest2);
    }

    void verifyFullVerificationData(const StoredDigest& key_digest, const char* data, size_t dataSize, bool noKey) {

        ASSERT(dataSize > (sizeof(CheckType) + 3 * sizeof(StoredDigest)));
        auto p = reinterpret_cast<const long*>(data);
        ASSERT(p[0] == static_cast<long>(CheckType::FULL_CHECK));

        // Construct a checksum over data including the unique digest

        MD5 md5;
        md5.add(data + sizeof(CheckType) + 2 * sizeof(StoredDigest),
                dataSize - sizeof(CheckType) - 2 * sizeof(StoredDigest));
        StoredDigest checksum_digest(md5);

        auto key_digest_stored = StoredDigest::load(data + sizeof(CheckType));
        auto checksum_digest_stored = StoredDigest::load(data + sizeof(CheckType) + sizeof(StoredDigest));

        if (!noKey) {
            ASSERT(key_digest == key_digest_stored);
        }
        ASSERT(checksum_digest == checksum_digest_stored);
    }
};


//----------------------------------------------------------------------------------------------------------------------

void HammerConfig::constructOptions(std::vector<eckit::option::Option*>& options) {

    // Parametrise the request, specifying the base request to run

    options.push_back(new eckit::option::SimpleOption<std::string>("expver", "Reset expver on data"));
    options.push_back(new eckit::option::SimpleOption<std::string>("class", "Reset class on data"));
    options.push_back(new eckit::option::SimpleOption<long>("step", "The first step number to use"));
    options.push_back(new eckit::option::SimpleOption<long>("number", "The first ensemble number to use"));
    options.push_back(new eckit::option::SimpleOption<long>("level", "The first level number to use"));

    // Specify the range of the execution

    options.push_back(new eckit::option::SimpleOption<long>("nsteps", "Number of steps"));
    options.push_back(new eckit::option::SimpleOption<long>("nensembles", "Number of ensemble members"));
    options.push_back(new eckit::option::SimpleOption<long>("nlevels", "Number of levels"));
    options.push_back(new eckit::option::SimpleOption<long>("nparams", "Number of parameters"));

    options.push_back(
        new eckit::option::SimpleOption<std::string>("levels", "Comma-separated list of level numbers to use"));

    // How are iterations progressed

    options.push_back(new eckit::option::SimpleOption<long>(
        "start-at", "Index from 0 to nlevels x nparams - 1 where to start iterating"));
    options.push_back(new eckit::option::SimpleOption<long>(
        "stop-at", "Index from 0 to nlevels x nparams - 1 where to stop iterating"));
    options.push_back(new eckit::option::SimpleOption<long>(
        "step-window",
        "Number of seconds allocated to perform I/O for a step, and slept if not consumed, for writers "
        "in ITT mode. Defaults to 10."));
    /// TODO(ssmart): Clarify the units here. This is a really weird description. It is ultimately a percentage?
    options.push_back(
        new eckit::option::SimpleOption<long>("random-delay",
                                              "Writers in ITT mode sleep for a random amount of time between 0 and "
                                              "step-window * random-delay / 100. Defaults to 100."));

    // Logging options

    options.push_back(new eckit::option::SimpleOption<bool>("verbose", "Print verbose output"));

    // Control of runtime

    options.push_back(new eckit::option::SimpleOption<bool>("read", "Read rather than write the data"));
    options.push_back(new eckit::option::SimpleOption<bool>("list", "List rather than write the data"));
    options.push_back(new eckit::option::SimpleOption<bool>("disable-subtocs", "Disable use of subtocs"));
    options.push_back(new eckit::option::SimpleOption<bool>(
        "md-check",
        "Calculate a metadata checksum (checksum of the field key + unique operation identifier) on "
        "every field write and insert it at the begginning and end of the encoded message data payload. "
        "On every field read, the checksums are extracted from the message data payload, the checksum of the FDB "
        "field key is recalculated, and all checksums are checked to match."));
    options.push_back(new eckit::option::SimpleOption<bool>(
        "full-check",
        "Insert a metadata checksum (checksum of the field key + unique operation identifier) and a "
        "checksum of the full field data payload, on every field write, at the begginning of the encoded message "
        "data payload. The data checksum includes the unique operation identifier in its calculation, and the "
        "resulting checksum is written at the beginning of the encoded message data payload between the field "
        "key checksum and the operation identifier. On every field read, the key checksum is extracted from the "
        "message data payload, the key checksum is recalculated from FDB field key, and both checksums are "
        "compared. The checksum of the data payload is also extracted and recalculated from the message data "
        "payload (excluding the key checksum but including the operation identifier), and both checksums are "
        "checked to match."));
    options.push_back(new eckit::option::SimpleOption<bool>(
        "no-randomise-data",
        "Disable randomisation of field data written. Written field data is randomised by default unless "
        " --full-check is supplied."));
    options.push_back(new eckit::option::SimpleOption<bool>("itt", "Run the benchmark in ITT mode"));
    options.push_back(new eckit::option::SimpleOption<long>(
        "check-queue-size",
        "How many messages should be stored in the message queue for asynchronous processing of checks. "
        "(default 10)"));

    /// TODO(ssmart): Provide a description of what this uri-file should contain. And ideally rename the option as
    ///               it is utterly non-descriptive.
    options.push_back(new eckit::option::SimpleOption<std::string>(
        "uri-file",
        "If the benchmark is invoked in ITT reader mode, "
        "the path to a file containing field URIs for the benchmark to read can be supplied via this argument. FDB "
        "listing is skipped."));

    // Specify parallelism/blocking options

    options.push_back(
        new eckit::option::SimpleOption<long>("ppn",
                                              "Number of processes per node the benchmark is being run on. Required "
                                              "for the ITT write mode to barrier after flush"));
    options.push_back(
        new eckit::option::SimpleOption<std::string>("nodes",
                                                     "Comma-separated list of nodes the benchmark is being run on. "
                                                     "Required for the ITT write mode to barrier after flush"));
    options.push_back(new eckit::option::SimpleOption<long>(
        "barrier-port",
        "Port number to use for TCP communications for the barrier in ITT write mode. Defaults to 7777"));
    options.push_back(new eckit::option::SimpleOption<long>(
        "barrier-max-wait", "Maximum amount of time to wait for the barrier in ITT write mode. Defaults to 10"));

    // Timing options

    options.push_back(new eckit::option::SimpleOption<bool>("delay", "Add random delay to writer startup"));
    options.push_back(new eckit::option::SimpleOption<long>(
        "poll-period", "Number of seconds to use as polling period for readers in ITT mode. Defaults to 1"));
    options.push_back(new eckit::option::SimpleOption<long>(
        "poll-max-attempts",
        "Number of list/polling attempts before failing for readers in ITT mode. Defaults to 200"));
}

HammerConfig HammerConfig::parse(const eckit::option::CmdArgs& args) {
    HammerConfig config;

    // Up-front validation

    if (!args.has("expver") || !args.has("class") || !args.has("nsteps") || !args.has("nparams")) {
        throw UserError("--expver, --class, --nsteps and --nparams are required arguments", Here());
    }

    bool itt = args.getBool("itt", false);
    if (!itt && !args.has("nlevels")) {
        throw UserError("--nlevels is a required argument unless --itt is specified", Here());
    }

    if (itt && !(args.has("nodes") && args.has("ppn") && (args.has("nlevels") || args.has("levels")))) {
        throw UserError(
            "--nodes, --ppn and an argument specifying levels are required when running in ITT mode (--itt)", Here());
    }

    // Fill in config values

    config.request = {
        args.getString("expver"),
        args.getString("class"),
        HammerConfig::parseLevelist(args),
        HammerConfig::parseParamlist(args),
        HammerConfig::parseEnsemblelist(args),
        HammerConfig::parseSteplist(args),
    };

    config.iteration = {
        args.getLong("start-at", 0),
        args.getLong("stop-at", config.request.levelist.size() * config.request.paramlist.size() - 1),
        args.getInt("step-window", 10),
        args.getInt("random-delay", 100),
        args.has("nensembles"),
    };

    config.execution = {
        parseMode(args),
        args(0),
        args.getBool("md-check", false),
        args.getBool("full-check", false),
        !args.getBool("no-randomise-data", false),
        itt,
        args.getString("uri-file", ""),
        args.getLong("check-queue-size", 11),
    };

    config.timing = {
        args.getBool("delay", false),
        args.getLong("poll-period", 1),
        args.getLong("poll-max-attempts", 200),
    };

    config.interface = {
        args.getBool("verbose", false),
    };

    config.parallel = {
        args.getLong("ppn", 1),
        Tokenizer(",").tokenize(args.getString("nodes", "")),
        args.getInt("barrier-port", 7777),
        args.getInt("barrier-max-wait", 10),
    };

    eckit::LocalConfiguration userConfig;
    userConfig.set("useSubToc", !args.getBool("disable-subtocs", false));
    config.fdbConfig = fdb5::FDBTool::config(args);

    // Config overrides

    if (config.execution.fullCheck) {
        config.execution.mdCheck = false;
    }

    // post-Validation

    // if (config.execution.mode == Mode::Write && config.execution.randomiseData && config.execution.fullCheck) {
    //     throw UserError("Cannot enable full consistency checks with data randomisation enabled", Here());
    // }

    long nparams = config.request.paramlist.size();
    long nlevels = config.request.levelist.size();
    ASSERT(config.iteration.startAtChunk < nlevels * nparams);
    ASSERT(config.iteration.stopAtChunk < nlevels * nparams);
    ASSERT(config.iteration.stopAtChunk >= config.iteration.startAtChunk);

    return config;
}

std::vector<long> HammerConfig::parseLevelist(const eckit::option::CmdArgs& args) {

    std::vector<long> levelist;

    if (args.has("levels")) {
        auto tokenized = Tokenizer(",").tokenize(args.getString("levels"));
        for (const auto& token : tokenized) {
            levelist.emplace_back(translate<long>(token));
        }
    }
    else {
        long level = args.getLong("level", 0);
        long nlevels = args.getLong("nlevels");
        levelist.reserve(nlevels);
        for (long i = 0; i < nlevels; ++i) {
            levelist.emplace_back(level + i);
        }
    }

    return levelist;
}

std::vector<long> HammerConfig::parseParamlist(const eckit::option::CmdArgs& args) {

    long nparams = args.getLong("nparams");
    if (nparams > VALID_PARAMS.size()) {
        throw UserError("Requested number of parameters exceeds available valid parameters", Here());
    }

    std::vector<long> paramlist;
    paramlist.reserve(nparams);
    for (long i = 0; i < nparams; ++i) {
        paramlist.emplace_back(VALID_PARAMS[i]);
    }
    return paramlist;
}

std::vector<long> HammerConfig::parseEnsemblelist(const eckit::option::CmdArgs& args) {

    long baseNumber = args.getLong("number", 1);
    long nensembles = args.getLong("nensembles", 1);

    std::vector<long> ensemblelist;
    ensemblelist.reserve(nensembles);
    for (long i = 0; i < nensembles; ++i) {
        ensemblelist.emplace_back(baseNumber + i);
    }

    return ensemblelist;
}

std::vector<long> HammerConfig::parseSteplist(const eckit::option::CmdArgs& args) {

    long baseStep = args.getLong("step", 0);
    long nsteps = args.getLong("nsteps");

    std::vector<long> steplist;
    steplist.reserve(nsteps);
    for (long i = 0; i < nsteps; ++i) {
        steplist.emplace_back(baseStep + i);
    }

    return steplist;
}

HammerConfig::Mode HammerConfig::parseMode(const eckit::option::CmdArgs& args) {

    bool read = args.getBool("read", false);
    bool list = args.getBool("list", false);

    if (read && list) {
        throw UserError("--read and --list options are mutually exclusive", Here());
    }

    if (read) {
        return Mode::Read;
    }
    else if (list) {
        return Mode::List;
    }
    else {
        return Mode::Write;
    }
}

metkit::mars::MarsRequest HammerConfig::templateRequest() const {

    fdb5::MessageDecoder decoder;
    auto requests = decoder.messageToRequests(execution.templateGrib);

    if (requests.size() != 1) {
        throw UserError("Template GRIB file must contain exactly one message", Here());
    }

    auto req = requests.front();
    req.setValue("expver", request.expver);
    req.setValue("class", request.class_);
    return req;
}


//----------------------------------------------------------------------------------------------------------------------


class FDBHammer : public fdb5::FDBTool {

    struct ReadStats {
        size_t totalRead = 0;
        size_t fieldsRead = 0;
        Timer totalTimer;
        Timer readTimer;
        Timer listTimer;
        size_t listAttempts = 0;
        struct ::timeval timeBeforeIO;
        struct ::timeval timeAfterIO;
    };

public:  // methods

    FDBHammer(int argc, char** argv);

    ~FDBHammer() override = default;

private:  // methods

    void usage(const std::string& tool) const override;
    void init(const eckit::option::CmdArgs& args) override;
    int numberOfPositionalArguments() const override { return 1; }

    void execute(const eckit::option::CmdArgs& args) override;
    void executeRead();
    void executeWrite();
    void executeList();

    std::pair<std::unique_ptr<DataHandle>, std::deque<Key>> constructReadData(std::optional<FDB>& fdb,
                                                                              ReadStats& stats);
    std::pair<std::unique_ptr<DataHandle>, std::deque<Key>> constructReadDataURIFile(std::optional<FDB>& fdb,
                                                                                     ReadStats& stats);
    std::pair<std::unique_ptr<DataHandle>, std::deque<Key>> constructReadDataStandard(std::optional<FDB>& fdb,
                                                                                      ReadStats& stats);
    std::pair<std::unique_ptr<DataHandle>, std::deque<Key>> constructReadDataITT(std::optional<FDB>& fdb,
                                                                                 ReadStats& stats);

private:  // members

    HammerConfig config_;
};

//----------------------------------------------------------------------------------------------------------------------

FDBHammer::FDBHammer(int argc, char** argv) : fdb5::FDBTool(argc, argv) {
    HammerConfig::constructOptions(options_);
}

void FDBHammer::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool
                       << " [--read] [--list] [--no-randomise-data] [--md-check|--full-check] [--statistics] "
                          "[--itt] [--step-window] [--random-delay] [--poll-period=<period>] [--uri-file=<path>] "
                          "[--poll-max-attempts=<attempts>] [--ppn=<ppn>] "
                          "[--nodes=<hostname1,hostname2,...>] [--barrier-port=<port>] [--barrier-max-wait=<seconds>] "
                          "--expver=<expver> --nparams=<nparams> "
                          "[--nlevels=<nlevels> [--level=<level>]]|[--levels=<level1,level2,...>] "
                          "--nensembles=<nensembles> [--number=<number>] "
                          "--nsteps=<nsteps> [--step=<step>] "
                          "<grib_path>"
                       << std::endl;
    fdb5::FDBTool::usage(tool);
}

void FDBHammer::init(const eckit::option::CmdArgs& args) {
    FDBTool::init(args);
    config_ = HammerConfig::parse(args);
}

namespace {

// TODO(ssmart): Create a barrier OBJECT which can maintain some of the state...

/// Local implementation of internode barriers using TCP sockets.
void barrier_internode(const std::vector<std::string>& nodes, int port, int max_wait) {

    std::string hostname = Main::hostname();
    if (nodes.size() == 1) {
        ASSERT(hostname == nodes[0]);
        return;
    }

    if (hostname == nodes[0]) {

        // this is the leader node
        // accept as many connections as peer nodes, on the designated port.

        eckit::net::TCPServer server(port);
        std::vector<eckit::net::TCPSocket> connections(nodes.size() - 1);

        for (int i = 0; i < connections.size(); ++i) {
            connections[i] = server.accept("Waiting for connection", max_wait);
        }

        // once all nodes are ready, send barrier end signal to all clients.

        const char message[] = {'E', 'N', 'D'};
        for (auto& socket : connections) {
            socket.write(&message[0], sizeof(message));
            socket.closeOutput();  /// ensure all data is sent
        }

        // the TCPSockets & TCPServer are auto-closed
    }
    else {

        // This is NOT the leader node

        /// @note if the server is not yet listening, the client connection wil fail and will be retriede evry second
        /// until timeout.
        /// This could be adjusted to either retry more frequently, or to maintain persistent connections to the
        /// master node to avoid the overhead of establishing the connection every time.

        eckit::net::TCPClient client;
        eckit::net::TCPSocket socket = client.connect(nodes[0], port, max_wait, 0, 1);

        // Wait for barrier end

        std::array<char, 3> message;
        long read = socket.read(message.data(), message.size());
        ASSERT(::memcmp("END", message.data(), message.size()) == 0);

        // TCPSocket is auto-closed
    }
}

void barrier(size_t ppn, const std::vector<std::string>& nodes, int port, int max_wait) {

    // If there is only one process per node, we don't need the intra-node barrier

    if (ppn == 1) {
        barrier_internode(nodes, port, max_wait);
        return;
    }

    bool leader_found = false;
    while (!leader_found) {

        uid_t uid = ::getuid();
        eckit::Translator<uid_t, std::string> uid_to_str;
        eckit::PathName default_run_path{"/var/run/user"};
        default_run_path /= uid_to_str(uid);

        eckit::PathName run_path(eckit::Resource<std::string>("$FDB_HAMMER_RUN_PATH", default_run_path));

        eckit::PathName wait_fifo = run_path;
        wait_fifo /= "fdb-hammer.wait.fifo";

        eckit::PathName barrier_fifo = run_path;
        barrier_fifo /= "fdb-hammer.barrier.fifo";

        /// attempt exclusive create of an application-unique file
        eckit::PathName pid_file = run_path;
        pid_file /= "fdb-hammer.pid";

        int fd;
        fd = ::open(pid_file.localPath(), O_EXCL | O_CREAT | O_WRONLY, 0666);
        if (fd < 0 && errno != EEXIST) {
            throw eckit::FailedSystemCall("open", Here(), errno);
        }

        if (fd >= 0) {

            /// if succeeded, this process is the leader

            /// a pair of FIFOs are created. One for clients to communicate the leader they are
            ///   waiting, and another to open in blocking mode until leader opens it for write
            ///   once it has synchronised with peer nodes
            if (wait_fifo.exists()) {
                wait_fifo.unlink();
            }
            SYSCALL(::mkfifo(wait_fifo.localPath(), 0666));

            if (barrier_fifo.exists()) {
                barrier_fifo.unlink();
            }
            SYSCALL(::mkfifo(barrier_fifo.localPath(), 0666));

            /// the leader PID is written into the file
            SYSCALL(::close(fd));
            std::unique_ptr<eckit::DataHandle> fh(pid_file.fileHandle());
            eckit::HandleStream hs{*fh};
            fh->openForWrite(eckit::Length(sizeof(long)));
            {
                eckit::AutoClose closer(*fh);
                hs << (long)::getpid();
            }

            /// a signal from each peer process in the node is received
            std::vector<char> message = {'S', 'I', 'G'};
            std::unique_ptr<eckit::DataHandle> fh_wait(wait_fifo.fileHandle());
            /// TODO: handle errors and no-replies on open as well as on read and unlink
            fh_wait->openForRead();
            {
                eckit::AutoClose closer(*fh_wait);
                size_t pending = ppn - 1;
                while (pending > 0) {
                    message = {'0', '0', '0'};
                    ASSERT(fh_wait->read(&message[0], message.size()) == message.size());
                    ASSERT(std::string(message.begin(), message.end()) == "SIG");
                    --pending;
                }
            }
            /// remove the wait fifo
            wait_fifo.unlink(false);

            /// once all processes in the node are ready, barrier with peer nodes
            std::exception_ptr eptr;
            try {
                eckit::Timer barrier_timer;
                barrier_timer.start();
                barrier_internode(nodes, port, max_wait);
                barrier_timer.stop();
                // barrier_timer.reset("Inter-node barrier");
            }
            catch (...) {
                eptr = std::current_exception();
            }

            /// once all nodes are ready, open the barrier fifo for write which will
            ///   release all waiting peer processes in the node
            std::unique_ptr<eckit::DataHandle> fh_barrier(barrier_fifo.fileHandle());
            /// TODO: handle errors and no-replies on open as well as unlink
            fh_barrier->openForWrite(eckit::Length(0));
            {
                eckit::AutoClose closer(*fh_barrier);
                /// if the inter-node barrier failed, notify the clients via the barrier fifo
                if (eptr) {
                    message = {'S', 'I', 'G'};
                    size_t pending = ppn - 1;
                    while (pending > 0) {
                        ASSERT(fh_barrier->write(&message[0], message.size()) == message.size());
                        --pending;
                    }
                }
            }

            /// remove the barrier fifo
            barrier_fifo.unlink(false);

            pid_file.unlink(false);

            /// throw if the inter-node barrier failed
            if (eptr) {
                std::rethrow_exception(eptr);
            }
        }
        else {

            /// otherwise, the process is a client

            /// the leader PID is read from the file
            std::unique_ptr<eckit::DataHandle> fh(pid_file.fileHandle());
            eckit::HandleStream hs{*fh};
            long pid;
            eckit::Length size = fh->openForRead();
            {
                eckit::AutoClose closer(*fh);
                /// the PID file may be empty if trying too soon
                if (size == eckit::Length(0)) {
                    fh->close();
                    ::sleep(1);
                    size = fh->openForRead();
                }
                if (size == eckit::Length(0)) {
                    throw eckit::SeriousBug("Found empty PID file. Leader too slow or manual remove required.");
                }
                hs >> pid;
            }
            pid_t leader_pid = (long)pid;

            /// the leadr PID is checked to exist. If not, clean up and
            /// restart election procedure
            if (::kill(leader_pid, 0) != 0) {
                try {
                    pid_file.unlink();
                }
                catch (eckit::FailedSystemCall& e) {
                    if (errno != ENOENT) {
                        throw;
                    }
                }
                continue;
            }

            /// wait for leader to open barrier FIFO to signal barrier end
            auto future = std::async(std::launch::async, [barrier_fifo]() {
                try {
                    /// TODO: handle errors and no-replies on opens as well as on write
                    /// NOTE: cannot use FileHandle.openForRead as it internally performs
                    ///   an estimate() on the fifo which may have been removed by leader.
                    int fd;
                    SYSCALL(fd = ::open(barrier_fifo.localPath(), O_RDONLY));
                    eckit::FileDescHandle fh_barrier(fd, true);

                    /// check for any errors reported by leader
                    std::vector<char> message = {'0', '0', '0'};
                    long bytes = fh_barrier.read(&message[0], message.size());
                    if (bytes > 0) {
                        ASSERT(bytes == message.size());
                        ASSERT(std::string(message.begin(), message.end()) == "SIG");
                        return false;
                    }

                    /// fd is autoclosed
                    return true;
                }
                catch (std::exception& e) {
                    return false;
                }
            });

            /// signal the leader this process is waiting
            std::vector<char> message = {'S', 'I', 'G'};
            std::unique_ptr<eckit::DataHandle> fh_wait(wait_fifo.fileHandle());
            // eckit::HandleStream hs_wait{*fh_wait};
            fh_wait->openForWrite(eckit::Length(sizeof(long)));
            eckit::AutoClose closer(*fh_wait);
            ASSERT(fh_wait->write(&message[0], message.size()) == message.size());

            if (!future.get()) {
                throw eckit::Exception("Error receiving response from barrier leader process.");
            }
        }

        leader_found = true;
    }
}

uint32_t xorshift(uint32_t& state) {
    state ^= (state << 13);
    state ^= (state >> 17);
    state ^= (state << 5);
    return state;
}

float generateRandomFloat(uint32_t& state) {
    uint32_t randomInt = xorshift(state);
    return static_cast<float>(randomInt) / std::numeric_limits<uint32_t>::max();
}

uint32_t generateRandomUint32() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;
    return dis(gen);
}
}  // namespace

void FDBHammer::execute(const eckit::option::CmdArgs&) {

    if (config_.execution.mode == HammerConfig::Mode::Read) {
        executeRead();
    }
    else if (config_.execution.mode == HammerConfig::Mode::List) {
        executeList();
    }
    else {
        executeWrite();
    }
}

void FDBHammer::executeWrite() {

    // Read in the template GRIB file as the source GRIB material

    eckit::AutoStdFile fin(config_.execution.templateGrib);

    int err;
    codes_handle* handle = codes_handle_new_from_file(nullptr, fin, PRODUCT_GRIB, &err);
    ASSERT(handle);


    if (config_.timing.startupDelay) {
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dist(0, 10000);  // delay between 0 and 10 seconds

        int delayDuration = dist(mt);
        std::this_thread::sleep_for(std::chrono::milliseconds(delayDuration));
    }

    fdb5::MessageArchiver archiver(fdb5::Key(), false, config_.interface.verbose, config_.fdbConfig);

    {
        size_t size = config_.request.expver.length();
        CODES_CHECK(codes_set_string(handle, "expver", config_.request.expver.c_str(), &size), 0);
        size = config_.request.class_.length();
        CODES_CHECK(codes_set_string(handle, "class", config_.request.class_.c_str(), &size), 0);
    }

    if (config_.request.ensemblelist.size() > 1) {
        /// @note: Usually, fdb-hammer is run on multiple nodes and concurrent processes.
        ///   It is recommended to have all writer processes in a node configured to archive
        ///   fields for a same member. If running on only a few nodes or a single node,
        ///   it is recommended to have a group of the processes run in every node configured
        ///   to archive fields for a same member.
        ///   The fdb-hammer-parallel scripts enforce these recommendations, and it would
        ///   therefore be a bug if an fdb-hammer process were configured to archive fields for
        ///   more than one member.
        ///   Only if fdb-hammer is run manually as a single process it would possibly be
        ///   sensible to have it configured to archive fields for multiple members.
        Log::warning() << "A writer fdb-hammer process has been configured to archive "
                       << "fields for multiple members (" << config_.request.ensemblelist.size() << ")." << std::endl;
    }

    eckit::Translator<size_t, std::string> str;

    struct timeval tval_before_io, tval_after_io;
    eckit::Timer timer;
    eckit::Timer gribTimer;
    double elapsed_grib = 0;
    size_t writeCount = 0;
    size_t bytesWritten = 0;
    int total_slept = 0;

    uint32_t random_seed = generateRandomUint32();
    long offsetBeforeData = 0, offsetAfterData = 0, numberOfValues = 0;

    // get message data offset
    CODES_CHECK(codes_get_long(handle, std::string("offsetBeforeData").c_str(), &offsetBeforeData), 0);
    CODES_CHECK(codes_get_long(handle, std::string("offsetAfterData").c_str(), &offsetAfterData), 0);
    CODES_CHECK(codes_get_long(handle, std::string("numberOfValues").c_str(), &numberOfValues), 0);

    std::vector<double> random_values;
    if (config_.execution.randomiseData) {
        random_values.resize(numberOfValues);
    }

    if (config_.execution.itt) {
        eckit::Timer barrier_timer;
        barrier_timer.start();
        //        Barrier.block();
        barrier(config_.parallel.ppn, config_.parallel.nodelist, config_.parallel.barrierPort,
                config_.parallel.barrierMaxWait);
        barrier_timer.stop();
        // barrier_timer.reset("Barrier pre-step 0");

        float delay_range = config_.iteration.stepWindow * (config_.iteration.randomDelay / 100);
        if (delay_range > 0) {
            std::random_device rd;
            std::mt19937 mt(rd());
            std::uniform_int_distribution<int> dist(0, delay_range);
            int delayDuration = dist(mt);
            ::sleep(delayDuration);
        }
    }

    ::timeval start_timestamp;
    ::gettimeofday(&start_timestamp, 0);
    ::timeval step_end_due_timestamp = start_timestamp;

    HammerVerifier verifier(config_);

    timer.start();

    for (const auto& istep : config_.request.steplist) {

        CODES_CHECK(codes_set_long(handle, "step", istep), 0);
        for (const auto& imember : config_.request.ensemblelist) {
            if (config_.iteration.hasEnsembles) {
                CODES_CHECK(codes_set_long(handle, "number", imember), 0);
            }
            size_t iter_count = 0;
            for (const auto& ilevel : config_.request.levelist) {
                CODES_CHECK(codes_set_long(handle, "level", ilevel), 0);

                if (iter_count > config_.iteration.stopAtChunk) {
                    break;
                }

                for (const auto& iparam : config_.request.paramlist) {

                    if (iter_count > config_.iteration.stopAtChunk) {
                        break;
                    }
                    if (iter_count++ < config_.iteration.startAtChunk) {
                        continue;
                    }

                    if (config_.interface.verbose) {
                        Log::info() << "Member: " << imember << ", step: " << istep << ", level: " << ilevel
                                    << ", param: " << iparam << std::endl;
                    }

                    CODES_CHECK(codes_set_long(handle, "paramId", iparam), 0);

                    if (config_.execution.randomiseData) {
                        //                        ASSERT(!config_.execution.fullCheck);

                        // randomise field data
                        for (int i = 0; i < numberOfValues; ++i) {
                            random_values[i] = generateRandomFloat(random_seed);
                        }

                        CODES_CHECK(
                            codes_set_double_array(handle, "values", random_values.data(), random_values.size()), 0);

                        // If using CCSDS, the size of the data may have been adjusted...
                        CODES_CHECK(codes_get_long(handle, std::string("offsetBeforeData").c_str(), &offsetBeforeData),
                                    0);
                        CODES_CHECK(codes_get_long(handle, std::string("offsetAfterData").c_str(), &offsetAfterData),
                                    0);
                    }

                    size_t messageSize = 0;
                    const char* buffer = nullptr;
                    CODES_CHECK(codes_get_message(handle, reinterpret_cast<const void**>(&buffer), &messageSize), 0);

                    // Metadata or full verification

                    verifier.storeVerificationData(istep, imember, ilevel, iparam,
                                                   &const_cast<char*>(buffer)[offsetBeforeData],
                                                   offsetAfterData - offsetBeforeData);

                    gribTimer.stop();
                    elapsed_grib += gribTimer.elapsed();

                    MemoryHandle dh(buffer, messageSize);

                    if (imember == config_.request.ensemblelist.front() && istep == config_.request.steplist.front() &&
                        (iter_count - 1 == config_.iteration.startAtChunk)) {
                        gettimeofday(&tval_before_io, NULL);
                    }
                    archiver.archive(dh);
                    writeCount++;
                    bytesWritten += messageSize;

                    gribTimer.start();
                }
            }

            gribTimer.stop();
            elapsed_grib += gribTimer.elapsed();
            archiver.flush();
            if (imember == config_.request.ensemblelist.back() && istep == config_.request.steplist.back()) {
                gettimeofday(&tval_after_io, NULL);
            }
            gribTimer.start();
        }

        if (config_.execution.itt) {

            /// sleep until the data for the next step is 'received' from the model, i.e.
            /// for as long as the target per-step wall-clock-time, minus the expected
            /// receival + aggregation time, minus the time spent on I/O
            if (config_.iteration.stepWindow > 0) {
                step_end_due_timestamp.tv_sec += config_.iteration.stepWindow;
                ::timeval current_timestamp;
                ::gettimeofday(&current_timestamp, 0);
                int remaining = step_end_due_timestamp.tv_sec - current_timestamp.tv_sec;
                if (remaining > 0) {
                    ::sleep(remaining);
                    // std::cout << "Waiting " << remaining << " seconds at end of step " << istep << std::endl;
                    total_slept += remaining;
                }
                if (remaining < 0) {
                    // throw eckit::Exception("Step window exceeded.");
                    eckit::Log::info() << "Step window exceeded by " << -remaining << " seconds" << std::endl;
                }
            }
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
    Log::info() << "Total rate: " << double(bytesWritten) / (timer.elapsed() * 1_MiB) << " MB / s" << std::endl;

    Log::info() << "Timestamp before first IO: " << (long int)tval_before_io.tv_sec << "." << std::setw(6)
                << std::setfill('0') << (long int)tval_before_io.tv_usec << std::endl;
    Log::info() << "Timestamp after last IO: " << (long int)tval_after_io.tv_sec << "." << std::setw(6)
                << std::setfill('0') << (long int)tval_after_io.tv_usec << std::endl;
    if (config_.execution.itt) {
        Log::info() << "Average time slept per step: " << total_slept / config_.request.steplist.size() << std::endl;
    }
}

std::pair<std::unique_ptr<DataHandle>, std::deque<Key>> FDBHammer::constructReadData(std::optional<FDB>& fdb,
                                                                                     FDBHammer::ReadStats& stats) {

    if (config_.execution.itt) {

        if (config_.request.steplist.size() > 1) {
            // @note: Usually, fdb-hammer in ITT mode is run on multiple nodes and concurrent
            //   processes. All reader processes in a node are set to retrieve fields for
            //   one step at a time. If there are more steps than reader nodes, the steps are
            //   processed in a round-robin fashion across the available nodes.
            //   The ITT benchmark scripts run a new set of fdb-hammer processes in every reader
            //   node for every subsequent step assigned to that node, and it would therefore be
            //   a bug if a single fdb-hammer reader process in ITT mode were configured to
            //   retrieve fields for multiple steps.
            Log::warning() << "A reader fdb-hammer process in ITT mode has been configured to "
                           << "retrieve fields for multiple steps (" << config_.request.steplist.size() << ")."
                           << std::endl;
        }

        if (config_.execution.uriFile.empty()) {
            return constructReadDataITT(fdb, stats);
        }
        else {
            // uriFile not empty
            return constructReadDataURIFile(fdb, stats);
        }
    }
    else {
        // Normal execution of the hammer
        return constructReadDataStandard(fdb, stats);
    }
}

std::pair<std::unique_ptr<DataHandle>, std::deque<Key>> FDBHammer::constructReadDataURIFile(std::optional<FDB>& fdb,
                                                                                            ReadStats& stats) {

    std::vector<eckit::URI> uris;
    std::ifstream in{config_.execution.uriFile};
    for (std::string line; getline(in, line);) {
        uris.push_back(eckit::URI{line});
    }

    // ASSERT(uris.size() == (nsteps * nensembles * levelist.size() * paramlist.size()));
    gettimeofday(&stats.timeBeforeIO, NULL);
    stats.fieldsRead = uris.size();

    // We still need to work out what fields are expected...
    // But ... the expected keys ARE NOT KNOWN. So fill with empty keys, and ignore their value.

    std::deque<Key> expected_keys;
    for (const auto& _ : uris) {
        expected_keys.emplace_back(Key{});
    }

    /*
    for (const auto& istep : config_.request.steplist) {
        for (const auto& imember : config_.request.ensemblelist) {
            size_t iter_count = 0;

            for (const auto& ilevel : config_.request.levelist) {
                if (iter_count > config_.iteration.stopAtChunk)
                    break;

                for (const auto& iparam : config_.request.paramlist) {
                    if (iter_count > config_.iteration.stopAtChunk)
                        break;
                    if (iter_count++ < config_.iteration.startAtChunk)
                        continue;

                    expected_keys.push_back(shortFDBKey(istep, imember, ilevel, iparam));
                }
            }
        }
    }*/


    return std::make_pair(std::unique_ptr<DataHandle>(fdb->read(uris)), std::move(expected_keys));
}

std::pair<std::unique_ptr<DataHandle>, std::deque<Key>> FDBHammer::constructReadDataStandard(std::optional<FDB>& fdb,
                                                                                             ReadStats& stats) {

    metkit::mars::MarsRequest request = config_.templateRequest();
    request.setValue("optimised", "on");

    HandleGatherer handles(false);
    std::deque<Key> expected_keys;

    gettimeofday(&stats.timeBeforeIO, NULL);

    for (const auto& istep : config_.request.steplist) {
        request.setValue("step", istep);

        for (const auto& imember : config_.request.ensemblelist) {
            if (config_.iteration.hasEnsembles) {
                request.setValue("number", imember);
            }

            size_t iter_count = 0;

            for (const auto& ilevel : config_.request.levelist) {
                request.setValue("levelist", ilevel);

                if (iter_count > config_.iteration.stopAtChunk) {
                    break;
                }

                for (const auto& iparam : config_.request.paramlist) {
                    request.setValue("param", iparam);

                    if (iter_count > config_.iteration.stopAtChunk) {
                        break;
                    }
                    if (iter_count++ < config_.iteration.startAtChunk) {
                        continue;
                    }

                    if (config_.interface.verbose) {
                        Log::info() << "Member: " << imember << ", step: " << istep << ", level: " << ilevel
                                    << ", param: " << iparam << std::endl;
                    }

                    handles.add(fdb->retrieve(request));
                    expected_keys.push_back(shortFDBKey(istep, imember, ilevel, iparam));
                    stats.fieldsRead++;
                }
            }
        }
    }

    return std::make_pair(std::unique_ptr<DataHandle>(handles.dataHandle()), std::move(expected_keys));
}

std::pair<std::unique_ptr<DataHandle>, std::deque<Key>> FDBHammer::constructReadDataITT(std::optional<FDB>& fdb,
                                                                                        ReadStats& stats) {

    // First we need to determine _which data we should be listing

    // Mutable versions of the config lists, so that we can manipulate them before the run

    auto levelist = to_stringvec(config_.request.levelist);
    const size_t nlevels = levelist.size();
    auto paramlist = to_stringvec(config_.request.paramlist);
    const size_t nparams = paramlist.size();

    // Build the list request(s) matching the whole domain,
    // optionally cropped via start_at and stop_at, to be used to fetch the field locations.
    // This manipulates the 'levelist' variable.

    std::vector<metkit::mars::MarsRequest> mars_list_requests;

    {
        metkit::mars::MarsRequest template_request = config_.templateRequest();
        template_request.setValuesTyped(new metkit::mars::TypeAny("number"),
                                        to_stringvec(config_.request.ensemblelist));

        size_t first_level = config_.iteration.startAtChunk / nparams;
        size_t last_level = config_.iteration.stopAtChunk / nparams;

        if (config_.iteration.startAtChunk > 0 && config_.iteration.stopAtChunk < nlevels * nparams - 1 &&
            first_level == last_level) {
            metkit::mars::MarsRequest body_list_request = template_request;

            std::vector<std::string> paramlist_body(paramlist.begin() + (config_.iteration.startAtChunk % nparams),
                                                    paramlist.begin() + (config_.iteration.stopAtChunk % nparams) + 1);
            body_list_request.setValuesTyped(new metkit::mars::TypeAny("param"), paramlist_body);

            std::vector<std::string> levelist_body(levelist.begin() + last_level, levelist.begin() + last_level + 1);
            body_list_request.setValuesTyped(new metkit::mars::TypeAny("levelist"), levelist_body);

            mars_list_requests.push_back(body_list_request);

            levelist.erase(levelist.begin(), levelist.end());
        }

        if (config_.iteration.stopAtChunk < nlevels * nparams - 1 && levelist.size() > 0) {
            metkit::mars::MarsRequest tail_list_request = template_request;

            std::vector<std::string> paramlist_tail(paramlist.begin(),
                                                    paramlist.begin() + (config_.iteration.stopAtChunk % nparams) + 1);
            tail_list_request.setValuesTyped(new metkit::mars::TypeAny("param"), paramlist_tail);

            std::vector<std::string> levelist_tail(levelist.begin() + last_level, levelist.begin() + last_level + 1);
            tail_list_request.setValuesTyped(new metkit::mars::TypeAny("levelist"), levelist_tail);

            mars_list_requests.push_back(tail_list_request);

            levelist.erase(levelist.begin() + last_level, levelist.end());
        }

        if (config_.iteration.startAtChunk > 0 && levelist.size() > 0) {
            metkit::mars::MarsRequest head_list_request = template_request;

            std::vector<std::string> paramlist_head(paramlist.begin() + (config_.iteration.startAtChunk % nparams),
                                                    paramlist.end());
            head_list_request.setValuesTyped(new metkit::mars::TypeAny("param"), paramlist_head);

            std::vector<std::string> levelist_head(levelist.begin() + first_level, levelist.begin() + first_level + 1);
            head_list_request.setValuesTyped(new metkit::mars::TypeAny("levelist"), levelist_head);

            mars_list_requests.push_back(head_list_request);

            levelist.erase(levelist.begin(), levelist.begin() + first_level + 1);
        }

        if (levelist.size() > 0) {
            metkit::mars::MarsRequest body_list_request = template_request;
            body_list_request.setValuesTyped(new metkit::mars::TypeAny("param"), paramlist);
            body_list_request.setValuesTyped(new metkit::mars::TypeAny("levelist"), levelist);
            mars_list_requests.push_back(body_list_request);
            levelist.erase(levelist.begin(), levelist.end());
        }
    }


    // Then we need to perform the list operations, to construct the DataHandle, and the expected
    // FDB key lists.

    metkit::mars::MarsRequest request = config_.templateRequest();
    request.setValue("optimised", "on");

    std::deque<Key> expected_keys;
    HandleGatherer handles(false);

    gettimeofday(&stats.timeBeforeIO, NULL);

    for (const auto& istep : config_.request.steplist) {
        request.setValue("step", istep);

        /// @note: ensure the step to retrieve data for has been fully
        ///   archived+flushed by listing all of its fields
        for (auto& mars_list_request : mars_list_requests) {
            mars_list_request.setValue("step", istep);
        }

        if (config_.interface.verbose) {
            for (auto& mars_list_request : mars_list_requests) {
                eckit::Log::info() << "Attempting list of " << mars_list_request << std::endl;
            }
        }

        size_t expected = 0;
        std::vector<fdb5::FDBToolRequest> list_requests;
        for (auto& mars_list_request : mars_list_requests) {
            list_requests.push_back(fdb5::FDBToolRequest{mars_list_request, false});
            expected += mars_list_request.count();
        }

        std::vector<eckit::URI> uris;
        std::deque<Key> step_expected_keys;
        bool dataReady = false;
        size_t attempts = 0;

        while (!dataReady) {
            uris.clear();
            step_expected_keys.clear();

            stats.listTimer.start();
            size_t count = 0;
            for (auto& list_request : list_requests) {
                auto listObject = fdb->list(list_request, true);
                fdb5::ListElement info;
                while (listObject.next(info)) {
                    if (config_.interface.verbose) {
                        Log::info() << info.keys()[2] << std::endl;
                    }
                    uris.push_back(info.location().fullUri());
                    fdb5::Key key = info.combinedKey();

                    step_expected_keys.push_back(
                        shortFDBKey(key.get("step"), key.get("number"), key.get("levelist"), key.get("param")));
                    ++count;
                }
            }
            stats.listTimer.stop();

            ++stats.listAttempts;
            ++attempts;
            if (count == expected) {
                dataReady = true;
            }
            else {
                if (config_.interface.verbose) {
                    eckit::Log::info() << "Expected " << expected << ", found " << count << std::endl;
                }
                if (attempts >= config_.timing.pollMaxAttempts) {
                    throw eckit::Exception(std::string("List maximum attempts (") +
                                           eckit::Translator<size_t, std::string>()(config_.timing.pollMaxAttempts) +
                                           ") exceeded");
                }
                ::sleep(config_.timing.pollPeriod);
                stats.listTimer.start();
                fdb.emplace(config_.fdbConfig);
                stats.listTimer.stop();
            }
        }

        handles.add(fdb->read(uris));
        stats.fieldsRead += uris.size();
        for (const auto& k : step_expected_keys) {
            expected_keys.emplace_back(k);
        }
    }

    return std::make_pair(std::unique_ptr<DataHandle>(handles.dataHandle()), std::move(expected_keys));
}

void FDBHammer::executeRead() {

    ReadStats stats;

    // We keep one instance of the FDB for all options, but hold it in an optional so that we can
    // recreate/re-destroy it on demand to purge all cached state when polling for data availability
    std::optional<fdb5::FDB> fdb;
    stats.listTimer.start();
    fdb.emplace(config_.fdbConfig);
    stats.listTimer.stop();

    // Work out what work we expect to do

    stats.totalTimer.start();
    auto [source_dh, expected_keys] = constructReadData(fdb, stats);

    // Short-circuit if there really are _no_ checks to perform

    stats.readTimer.start();
    if (!(config_.execution.fullCheck || config_.execution.mdCheck)) {
        EmptyHandle nullOutputHandle;
        stats.totalRead = source_dh->saveInto(nullOutputHandle);
        stats.readTimer.stop();
        stats.totalTimer.stop();
    }
    else {
        HammerVerifier verifier(config_, std::move(expected_keys));

        message::Reader reader(*source_dh);
        message::Message msg;
        while ((msg = reader.next())) {
            stats.totalRead += msg.length();
            verifier.verifyMessage(std::move(msg));
        }

        stats.readTimer.stop();
        stats.totalTimer.stop();  // exclude any leftover verification
        verifier.readComplete();
    }

    gettimeofday(&stats.timeAfterIO, NULL);

    // Pretty output

    if (config_.execution.itt) {
        Log::info() << "Time spent on " << stats.listAttempts << " list attempts: " << stats.listTimer.elapsed() << " s"
                    << std::endl;
    }
    Log::info() << "Data read duration: " << stats.readTimer.elapsed() << std::endl;
    Log::info() << "Fields read: " << stats.fieldsRead << std::endl;
    Log::info() << "Bytes read: " << stats.totalRead << std::endl;
    Log::info() << "Total duration: " << stats.totalTimer.elapsed() << std::endl;
    Log::info() << "Total rate: " << double(stats.totalRead) / stats.totalTimer.elapsed() << " bytes / s" << std::endl;
    Log::info() << "Total rate: " << double(stats.totalRead) / (stats.totalTimer.elapsed() * 1_MiB) << " MB / s"
                << std::endl;

    Log::info() << "Timestamp before first IO: " << (long int)stats.timeBeforeIO.tv_sec << "." << std::setw(6)
                << std::setfill('0') << (long int)stats.timeBeforeIO.tv_usec << std::endl;
    Log::info() << "Timestamp after last IO: " << (long int)stats.timeAfterIO.tv_sec << "." << std::setw(6)
                << std::setfill('0') << (long int)stats.timeAfterIO.tv_usec << std::endl;
}


void FDBHammer::executeList() {

    // Construct list request

    metkit::mars::MarsRequest request = config_.templateRequest();
    request.values("levelist", to_stringvec(config_.request.levelist));
    request.values("param", to_stringvec(config_.request.paramlist));
    request.values("number", to_stringvec(config_.request.ensemblelist));

    eckit::LocalConfiguration userConfig;

    // Do the actual iteration

    eckit::Timer timer;
    timer.start();

    const std::vector<std::string> minimumKeys = {"class", "expver"};
    fdb5::FDB fdb(config_.fdbConfig);

    size_t count = 0;
    for (const auto& step : config_.request.steplist) {

        request.setValue("step", step);

        fdb5::ListElement info;
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

}  // namespace fdb5::tools


int main(int argc, char** argv) {
    fdb5::tools::FDBHammer app(argc, argv);
    return app.start();
}
