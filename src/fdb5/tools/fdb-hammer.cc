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
#include <iomanip>
#include <algorithm>
#include <random>
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
#include "eckit/io/FileDescHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/utils/Literals.h"

#include "metkit/mars/TypeAny.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageArchiver.h"
#include "fdb5/tools/FDBTool.h"

#include <unistd.h>
#include <limits.h>
#include "eckit/log/TimeStamp.h"
#include "eckit/utils/MD5.h"
#include "eckit/message/Reader.h"
#include "eckit/message/Message.h"

#include <fcntl.h>
#include <signal.h>
#include "eckit/utils/Literals.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/serialisation/HandleStream.h"

// Valid type=pf,levtype=pl parameters. This allows for up to 174 parameters.
const std::vector<size_t> VALID_PARAMS{1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 32, 33, 35, 36, 37, 38, 39, 40, 41, 42, 43, 46, 53, 54, 60, 62, 63, 64, 65, 66, 67, 70, 71, 74, 75, 76, 77, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 123, 125, 126, 127, 128, 129, 130, 131, 132, 133, 135, 138, 139, 140, 141, 145, 148, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 170, 171, 174, 183, 184, 185, 186, 187, 188, 190, 191, 192, 193, 194, 197, 198, 199, 200, 203, 204, 205, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 233, 236, 237, 238, 241, 242, 243, 246, 247, 248, 249, 250, 251, 252, 253, 254};

using namespace eckit;
using namespace eckit::literals;

class FDBHammer : public fdb5::FDBTool {

    void usage(const std::string& tool) const override;

    void init(const eckit::option::CmdArgs& args) override;

    int minimumPositionalArguments() const override { return 1; }

    void execute(const eckit::option::CmdArgs& args) override;

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
        options_.push_back(new eckit::option::SimpleOption<long>("nsteps", "Number of steps"));
        options_.push_back(new eckit::option::SimpleOption<long>("step", "The first step number to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nensembles", "Number of ensemble members"));
        options_.push_back(new eckit::option::SimpleOption<long>("number", "The first ensemble number to use"));
        options_.push_back(new eckit::option::SimpleOption<long>("nlevels", "Number of levels"));
        options_.push_back(new eckit::option::SimpleOption<long>("level", "The first level number to use"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("levels", "Comma-separated list of level numbers to use"));
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
        options_.push_back(new eckit::option::SimpleOption<bool>("itt", "Run the benchmark in ITT mode"));
        options_.push_back(new eckit::option::SimpleOption<long>("poll-period",
             "Number of seconds to use as polling period for readers in ITT mode. Defaults to 1"));
        options_.push_back(new eckit::option::SimpleOption<long>("ppn",
             "Number of processes per node the benchmark is being run on. Required for the ITT write mode to barrier after flush"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("nodes",
             "Comma-separated list of nodes the benchmark is being run on. Required for the ITT write mode to barrier after flush"));
        options_.push_back(new eckit::option::SimpleOption<long>("barrier-port",
             "Port number to use for TCP communications for the barrier in ITT write mode. Defaults to 7777"));
        options_.push_back(new eckit::option::SimpleOption<long>("barrier-max-wait",
             "Maximum amount of time to wait for the barrier in ITT write mode. Defaults to 10"));
        options_.push_back(new eckit::option::SimpleOption<bool>("delay", "Add random delay"));
    }
    ~FDBHammer() override {}

private:

    bool verbose_;
    bool itt_;
    bool md_check_;
    bool full_check_;
    bool step_barrier_;
};

void FDBHammer::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool
                       << " [--read] [--list] [--md-check|--full-check] [--statistics] "
                          "[--itt] [--poll-period=<period>] [--ppn=<ppn>] "
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

    ASSERT(args.has("expver"));
    ASSERT(args.has("class"));
    ASSERT(args.has("nsteps"));
    ASSERT(args.has("nparams"));

    verbose_ = args.getBool("verbose", false);

    itt_ = args.getBool("itt", false);

    md_check_ = args.getBool("md-check", false);
    full_check_ = args.getBool("full-check", false);
    if (full_check_) md_check_ = false;

    if (itt_ && full_check_)
        throw eckit::Exception("Cannot enable full consistency checks in ITT mode as it randomises data.");

    if (!itt_) ASSERT(args.has("nlevels"));

    step_barrier_ = false;

}

void barrier_internode(std::vector<std::string>& nodes, int& port, int& max_wait) {

    std::string hostname = Main::hostname();

    if (nodes.size() == 1) {
        ASSERT(hostname == nodes[0]);
        return;
    }

    if (hostname == nodes[0]) {

        /// this is the leader node

        /// accept as many connections as peer nodes, on the designated port.
        eckit::net::TCPServer server(port);
        // server.bind();
        std::vector<eckit::net::TCPSocket> connections(nodes.size() - 1);
        int i = 0;
        for (auto it = std::next(nodes.begin()); it != nodes.end(); ++it) {
            /// TODO: what if a few clients were accepted already, and the following accept
            ///   times out? Will accepted clients hang in socket.read?
            connections[i] = server.accept("Waiting for connection", max_wait);
            eckit::net::TCPSocket& socket = connections[i];
            ++i;
        }

        /// once all nodes are ready, send barrier end signal to all clients.
        /// create signal message
        std::vector<char> message{'E', 'N', 'D'};
        /// send it
        for (auto& socket : connections) {
            socket.write(&message[0], message.size());
            /// ensure all data is sent
            socket.closeOutput();
        }

        /// TODO: ensure client received all data?
        // for (auto& socket : connections) {
        //     ///   This waits for client to receive FIN and close connection.
        //     ASSERT(socket.read(message, size) == 0);
        // }

        /// the TCPSockets are auto-closed
        /// the TCPServer is auto-closed

    } else {

        /// this is a client node

        /// open connection and send message
        eckit::net::TCPClient client{};
        // client.bind();

        /// TODO: currently, if the server is not yet listening, the client connection will
        ///   fail and will be retried every seconds until timeout.
        ///   It could be made to retry more frequently to make the inter-node barrier faster,
        ///   but this would put strain on the network.
        ///   A better approach would be to keep connections between the leader and the
        ///   clients open to avoid the overhead of establishing the connections every
        ///   time the parallel processes need to barrier.
        int timeout = max_wait;
        eckit::net::TCPSocket socket;
        while (timeout > 0) {
            try {
                socket = client.connect(nodes[0], port, 0);
                break;
            } catch (eckit::TooManyRetries& e) {
                ::sleep(1);
                timeout -= 1;
            }
        }
        if (timeout == 0) throw eckit::TimeOut("Exceeded time limit waiting for connection to barrier leader socket.", max_wait);

        /// wait for barrier end
        long size = 3;
        std::vector<char> message(size);
        /// TODO: timeout at max_wait in case the connection is too slow or flaky.
        ///   Use eckit Resources useSelectOnTCPSocket and socketSelectTimeout?
        long read = socket.read(&message[0], size);
        std::string expected = "END";
        ASSERT(std::string(message.begin(), message.end()) == expected);

        /// TCPSocket is auto-closed

    }
}

void barrier(size_t& ppn, std::vector<std::string>& nodes, int& port, int& max_wait) {

    //return;

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
        if (fd < 0 && errno != EEXIST) throw eckit::FailedSystemCall("open", Here(), errno);

        if (fd >= 0) {

            /// if succeeded, this process is the leader

            /// a pair of FIFOs are created. One for clients to communicate the leader they are
            ///   waiting, and another to open in blocking mode until leader opens it for write
            ///   once it has synchronised with peer nodes
            if (wait_fifo.exists()) wait_fifo.unlink();
            SYSCALL(::mkfifo(wait_fifo.localPath(), 0666));

            if (barrier_fifo.exists()) barrier_fifo.unlink();
            SYSCALL(::mkfifo(barrier_fifo.localPath(), 0666));

            /// the leader PID is written into the file
            SYSCALL(::close(fd));
            std::unique_ptr<eckit::DataHandle> fh(pid_file.fileHandle());
            eckit::HandleStream hs{*fh};
            fh->openForWrite(eckit::Length(sizeof(long)));
            {
                eckit::AutoClose closer(*fh);
                hs << (long) ::getpid();
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
                //barrier_timer.reset("Inter-node barrier");
            } catch (...) {
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
            if (eptr) std::rethrow_exception(eptr);

        } else {

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
            pid_t leader_pid = (long) pid;

            /// the leadr PID is checked to exist. If not, clean up and
            /// restart election procedure
            if (::kill(leader_pid, 0) != 0) {
                try {
                    pid_file.unlink();
                } catch (eckit::FailedSystemCall& e) {
                    if (errno != ENOENT) throw;
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
                } catch (std::exception& e) {
                    return false;
                }
            });

            /// signal the leader this process is waiting
            std::vector<char> message = {'S', 'I', 'G'};
            std::unique_ptr<eckit::DataHandle> fh_wait(wait_fifo.fileHandle());
            //eckit::HandleStream hs_wait{*fh_wait};
            fh_wait->openForWrite(eckit::Length(sizeof(long)));
            eckit::AutoClose closer(*fh_wait);
            ASSERT(fh_wait->write(&message[0], message.size()) == message.size());

            if (!future.get()) throw eckit::Exception("Error receiving response from barrier leader process.");

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

    size_t nsteps      = args.getLong("nsteps");
    size_t step        = args.getLong("step", 0);
    size_t nensembles  = args.getLong("nensembles", 1);
    size_t number      = args.getLong("number", 1);
    size_t nlevels     = args.getLong("nlevels");
    size_t level       = args.getLong("level", 1);
    std::string levels = args.getString("levels", "");
    size_t nparams     = args.getLong("nparams");
    bool delay         = args.getBool("delay", false);

    ASSERT(nparams <= VALID_PARAMS.size());

    if (itt_) {
        ASSERT(args.has("nodes"));
        ASSERT(args.has("ppn"));
        ASSERT(args.has("nlevels") || args.has("levels"));
    }
    std::string nodes    = args.getString("nodes", "");
    size_t ppn           = args.getLong("ppn", 1);
    int port             = args.getInt("barrier-port", 7777);
    int max_wait         = args.getInt("barrier-max-wait", 10);

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

    std::vector<size_t> levelist;
    if (args.has("levels")) {
        for (const auto& element : eckit::Tokenizer(",").tokenize(levels)) {
            levelist.push_back(eckit::Translator<std::string, size_t>()(element));
        }
        nlevels = levelist.size();
        level = levelist[0];
    } else {
        for (size_t ilevel = level; ilevel <= nlevels + level - 1; ++ilevel) {
            levelist.push_back(ilevel);
        }
    }

    std::vector<std::string> nodelist;
    if (args.has("nodes")) {
        for (const auto& element : eckit::Tokenizer(",").tokenize(nodes)) {
            nodelist.push_back(element);
        }
    }

    fdb5::MessageArchiver archiver(fdb5::Key(), false, verbose_, config(args, userConfig));

    std::string expver = args.getString("expver");
    size               = expver.length();
    CODES_CHECK(codes_set_string(handle, "expver", expver.c_str(), &size), 0);
    std::string cls = args.getString("class");
    size            = cls.length();
    CODES_CHECK(codes_set_string(handle, "class", cls.c_str(), &size), 0);

    if (nensembles > 1) {
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
        Log::warning() << "A writer fdb-hammer process has been configured to archive " <<
            "fields for multiple members (" << nensembles << ")." << std::endl;
    }

    eckit::Translator<size_t,std::string> str;

    struct timeval tval_before_io, tval_after_io;
    eckit::Timer timer;
    eckit::Timer gribTimer;
    double elapsed_grib = 0;
    size_t writeCount   = 0;
    size_t bytesWritten = 0;

    uint32_t random_seed = generateRandomUint32();
    long offsetBeforeData = 0, offsetAfterData = 0, numberOfValues = 0;

    // get message data offset
    CODES_CHECK(codes_get_long(handle, std::string("offsetBeforeData").c_str(), &offsetBeforeData), 0);
    CODES_CHECK(codes_get_long(handle, std::string("offsetAfterData").c_str(), &offsetAfterData), 0);
    CODES_CHECK(codes_get_long(handle, std::string("numberOfValues").c_str(), &numberOfValues), 0);

    std::vector<double> random_values;
    if (!full_check_) {
        random_values.resize(numberOfValues);
    }

    int step_time = 25;
    int per_step_compute_time = 5;
    if (itt_) {
        eckit::Timer barrier_timer;
        barrier_timer.start();
        barrier(ppn, nodelist, port, max_wait);
        barrier_timer.stop();
        //barrier_timer.reset("Barrier pre-step 0");

        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dist(0, step_time);
        int delayDuration = dist(mt);
        ::sleep(delayDuration);
    }

    ::timeval start_timestamp;
    ::gettimeofday(&start_timestamp, 0);
    ::timeval step_end_due_timestamp = start_timestamp;

    timer.start();

    for (size_t istep = step; istep < nsteps + step; ++istep) {

        /// sleep for as long as the I/O server processes are expected to spend
        /// on field receival and aggregation
        if (itt_) ::sleep(per_step_compute_time);

        CODES_CHECK(codes_set_long(handle, "step", istep), 0);
        for (size_t member = number; member <= nensembles + number - 1; ++member) {
            if (args.has("nensembles")) {
                CODES_CHECK(codes_set_long(handle, "number", member), 0);
            }
            for (const auto& ilevel : levelist) {
                CODES_CHECK(codes_set_long(handle, "level", ilevel), 0);
                for (size_t param = 0; param < nparams; ++param) {

                    if (verbose_) {
                        Log::info() << "Member: " << member << ", step: " << istep << ", level: " << ilevel
                                   << ", param: " << VALID_PARAMS[param] << std::endl;
                    }

                    CODES_CHECK(codes_set_long(handle, "paramId", VALID_PARAMS[param]), 0);

                    if (!full_check_) {

                        // randomise field data
                        for (int i = 0; i < numberOfValues; ++i) {
                            random_values[i] = generateRandomFloat(random_seed);
                        }

                        CODES_CHECK(codes_set_double_array(handle, "values", random_values.data(), random_values.size()), 0);

                    }
                    CODES_CHECK(codes_get_message(handle, reinterpret_cast<const void**>(&buffer), &size), 0);

                    if (full_check_ or md_check_) {

                        // generate a checksum of the FDB key
                        fdb5::Key key({
                            {"number", str(member)},
                            {"step", str(istep)},
                            {"level", str(ilevel)},
                            {"param", str(param)},
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
            if (member == (nensembles + number - 1) && istep == (step + nsteps - 1))
                gettimeofday(&tval_after_io, NULL);
            gribTimer.start();
        }

        if (itt_) {

            if (step_barrier_) {
                eckit::Timer barrier_timer;
                barrier_timer.start();
                barrier(ppn, nodelist, port, max_wait);
                barrier_timer.stop();
                //barrier_timer.reset(std::string("Barrier post-step ") + str(istep));
            }

            /// sleep until the data for the next step is 'received' from the model, i.e.
            /// for as long as the target per-step wall-clock-time, minus the expected
            /// receival + aggregation time, minus the time spent on I/O
            step_end_due_timestamp.tv_sec += step_time;
            ::timeval current_timestamp;
            ::gettimeofday(&current_timestamp, 0);
            int remaining = step_end_due_timestamp.tv_sec - current_timestamp.tv_sec;
            if (remaining > 0) {
                ::sleep(remaining);
                std::cout << "Waiting " << remaining << " seconds at end of step " << istep << std::endl;
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
}


void FDBHammer::executeRead(const eckit::option::CmdArgs& args) {

    fdb5::MessageDecoder decoder;
    std::vector<metkit::mars::MarsRequest> requests = decoder.messageToRequests(args(0));

    ASSERT(requests.size() == 1);
    metkit::mars::MarsRequest request = requests[0];

    size_t nsteps      = args.getLong("nsteps");
    size_t step        = args.getLong("step", 0);
    size_t nensembles  = args.getLong("nensembles", 1);
    size_t number      = args.getLong("number", 1);
    size_t nlevels;
    size_t level;
    std::string levels = args.getString("levels", "");
    size_t nparams     = args.getLong("nparams");
    size_t poll_period = args.getLong("poll-period", 1);

    ASSERT(nparams <= VALID_PARAMS.size());

    request.setValue("expver", args.getString("expver"));
    request.setValue("class", args.getString("class"));
    request.setValue("optimised", "on");

    eckit::LocalConfiguration userConfig{};
    if (!args.has("disable-subtocs"))
        userConfig.set("useSubToc", true);

    eckit::Translator<size_t,std::string> str;

    std::vector<std::string> levelist;
    if (itt_) {
        ASSERT(args.has("levels"));
        for (const auto& element : eckit::Tokenizer(",").tokenize(levels)) {
            levelist.push_back(element);
        }
        nlevels = levelist.size();
        level = eckit::Translator<std::string, size_t>()(levelist[0]);
    } else {
        nlevels = args.getLong("nlevels");
        level   = args.getLong("level", 1);
        for (size_t ilevel = level; ilevel <= nlevels + level - 1; ++ilevel) {
            levelist.push_back(str(ilevel));
        }
    }

    std::vector<std::string> paramlist;
    for (size_t param = 0; param < nparams; ++param) {
        paramlist.push_back(str(VALID_PARAMS[param]));
    }
    if (itt_) {
        auto rng = std::default_random_engine {};
        std::shuffle(std::begin(paramlist), std::end(paramlist), rng);
    }

    if (itt_ && nsteps > 1) {
        /// @note: Usually, fdb-hammer in ITT mode is run on multiple nodes and concurrent
        ///   processes. All reader processes in a node are set to retrieve fields for
        ///   one step at a time. If there are more steps than reader nodes, the steps are
        ///   processed in a round-robin fashion across the available nodes.
        ///   The ITT benchmark scripts run a new set of fdb-hammer processes in every reader
        ///   node for every subsequent step assigned to that node, and it would therefore be
        ///   a bug if a single fdb-hammer reader process in ITT mode were configured to
        ///   retrieve fields for multiple steps.
        Log::warning() << "A reader fdb-hammer process in ITT mode has been configured to " <<
            "retrieve fields for multiple steps (" << nsteps << ")." << std::endl;
    }

    struct timeval tval_before_io, tval_after_io;
    eckit::Timer timer;
    timer.start();

    metkit::mars::MarsRequest mars_list_request = requests[0];

    mars_list_request.setValue("expver", args.getString("expver"));
    mars_list_request.setValue("class", args.getString("class"));

    std::vector<std::string> numberlist;
    for (size_t n = number; n <= number + nensembles - 1; ++n) {
        numberlist.push_back(str(n));
    }

    mars_list_request.setValuesTyped(new metkit::mars::TypeAny("param"), paramlist);
    mars_list_request.setValuesTyped(new metkit::mars::TypeAny("levelist"), levelist);
    mars_list_request.setValuesTyped(new metkit::mars::TypeAny("number"), numberlist);

    fdb5::HandleGatherer handles(false);
    std::optional<fdb5::FDB> fdb;
    fdb.emplace(config(args, userConfig));
    size_t fieldsRead = 0;

    for (size_t istep = step; istep < nsteps + step; ++istep) {
        request.setValue("step", step);
        if (itt_) {
            /// @note: ensure the step to retrieve data for has been fully
            ///   archived+flushed by listing all of its fields
            mars_list_request.setValue("step", step);
            if (verbose_) {
                eckit::Log::info() << "Attempting list of " << mars_list_request << std::endl;
            }
            fdb5::FDBToolRequest list_request{mars_list_request, false};
            bool dataReady = false;
            while (!dataReady) {
                auto listObject = fdb->list(list_request, true);
                size_t count = 0;
                fdb5::ListElement info;
                while (listObject.next(info)) ++count;
                if (count == mars_list_request.count()) {
                    dataReady = true;
                } else {
                    if (verbose_) {
                        eckit::Log::info() << "Expected " << mars_list_request.count() << ", found " << count << std::endl;
                    }
                    ::sleep(poll_period);
                    fdb.emplace(config(args, userConfig));
                }
            }
        }
        for (size_t member = number; member <= nensembles + number - 1; ++member) {
            if (args.has("nensembles")) {
                request.setValue("number", member);
            }
            for (const auto& ilevel : levelist) {
                request.setValue("levelist", ilevel);
                for (const auto& param : paramlist) {
                    request.setValue("param", param);

                    if (verbose_) {
                        Log::info() << "Member: " << member << ", step: " << istep << ", level: " << ilevel
                                    << ", param: " << param << std::endl;
                    }

                    if (member == number && istep == step && ilevel == str(level) && param == str(1)) {
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

        for (size_t istep = step; istep < nsteps + step; ++istep) {
            for (size_t member = number; member <= nensembles + number - 1; ++member) {
                for (const auto& ilevel : levelist) {
                    for (const auto& param : paramlist) {

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
                            {"number", str(member)},
                            {"step", str(istep)},
                            {"level", ilevel},
                            {"param", param},
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
    Log::info() << "Total rate: " << double(total) / (timer.elapsed() * 1_MiB) << " MB / s" << std::endl;

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

    ASSERT(nparams <= VALID_PARAMS.size());

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
    for (size_t param = 0; param < nparams; ++param) {
        param_values.push_back(std::to_string(VALID_PARAMS[param]));
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
