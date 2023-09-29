/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Sep 2023

#include <cmath>
#include <iomanip>

// #include "eckit/io/Length.h"
#include "eckit/log/BigNum.h"
// #include "eckit/log/Bytes.h"
// #include "eckit/log/Timer.h"
// #include "eckit/thread/AutoLock.h"

// #include "multio/LibMultio.h"
#include "fdb5/daos/DaosIOStats.h"

static const int FORMAT_WIDTH = 75;


// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosIOStats::DaosIOStats(const std::string& prefix) :
    prefix_(prefix) {
    // numReads_(0),
    // bytesRead_(0),
    // sumBytesReadSquared_(0),
    // sumReadTimesSquared_(0),
    // numWrites_(0),
    // bytesWritten_(0),
    // sumBytesWrittenSquared_(0),
    // sumWriteTimesSquared_(0),
    // numFlush_(0),
    // sumFlushTimesSquared_(0)
    // numArcCatKvOpen_(0),
    // sumArcCatKvOpenTimesSquared_(0),
    // numArcCatKvCheck_(0),
    // sumArcCatKvCheckTimesSquared_(0),
    // numArcIdxKvCreate_(0),
    // sumArcIdxKvCreateTimesSquared_(0),
    // numArcIdxKvPutKey_(0),
    // sumArcIdxKvPutKeyTimesSquared_(0),
    // numArcIdxKvPutIndex_(0),
    // sumArcIdxKvPutIndexTimesSquared_(0),
    // numArcCatKvGetIndex_(0),
    // sumArcCatKvGetIndexTimesSquared_(0),
    // numArcCatKvClose_(0),
    // sumArcCatKvCloseTimesSquared_(0) {

    if (!prefix_.empty())
        prefix_ += std::string(" ");

}

DaosIOStats::~DaosIOStats() {}

void DaosIOStats::logMdOperation(const std::string& label, eckit::Timer& timer) {

    std::map<std::string, MetadataOperationStats>::iterator it = md_stats_.find(label);

    if (it == md_stats_.end()) {
        it = md_stats_.insert(std::make_pair(std::move(label), MetadataOperationStats())).first;
    }

    it->second.numOps++;
    it->second.timing += timer;

    double elapsed = timer.elapsed();
    it->second.sumTimesSquared += elapsed * elapsed;

}

//     void logArcCatKvOpen(eckit::Timer& timer);
//     void logArcCatKvCheck(eckit::Timer& timer);
//     void logArcIdxKvCreate(eckit::Timer& timer);
//     void logArcIdxKvPutKey(eckit::Timer& timer);
//     void logArcIdxKvPutIndex(eckit::Timer& timer);
//     void logArcCatKvGetIndex(eckit::Timer& timer);
//     void logArcCatKvClose(eckit::Timer& timer);

// void DaosIOStats::logArcCatKvOpen(eckit::Timer& timer) {

//     numArcCatKvOpen_++;
//     arcCatKvOpenTiming_ += timer;

//     double elapsed = timer.elapsed();
//     sumArcCatKvOpenTimesSquared_ += elapsed * elapsed;

// }

// void IOStats::logRead(const Length& size, Timer& timer) {

//     numReads_++;
//     bytesRead_ += size;
//     sumBytesReadSquared_ += (size * size);
//     readTiming_ += timer;

//     double elapsed = timer.elapsed();
//     sumReadTimesSquared_ += elapsed * elapsed;

//     LOG_DEBUG_LIB(LibMultio) << "Read count: " << numReads_ << ", size: " << Bytes(size)
//                              << ", total: " << Bytes(bytesRead_) << ", time: " << elapsed << "s"
//                              << ", total: " << readTiming_.elapsed_ << "s" << std::endl;
// }


// void IOStats::logWrite(const Length& size, Timer& timer) {

//     numWrites_++;
//     bytesWritten_ += size;
//     sumBytesWrittenSquared_ += (size * size);
//     writeTiming_ += timer;

//     double elapsed = timer.elapsed();
//     sumWriteTimesSquared_ += elapsed * elapsed;

//     LOG_DEBUG_LIB(LibMultio) << "Write count: " << numWrites_ << ", size: " << Bytes(size)
//                              << ", total: " << Bytes(bytesWritten_) << ", time: " << elapsed << "s"
//                              << ", total: " << writeTiming_.elapsed_ << "s" << std::endl;
// }


// void IOStats::logFlush(Timer& timer) {

//     numFlush_++;
//     flushTiming_ += timer;

//     double elapsed = timer.elapsed();
//     sumFlushTimesSquared_ += elapsed * elapsed;

//     LOG_DEBUG_LIB(LibMultio) << "Flush count: " << numFlush_ << ", time: " << elapsed << "s"
//                              << ", total: " << flushTiming_.elapsed_ << "s" << std::endl;
// }

void DaosIOStats::report(std::ostream& s) const {

    // // Write statistics

    // reportCount(s, "num writes", numWrites_);
    // reportBytes(s, "bytes written", numWrites_, bytesWritten_, sumBytesWrittenSquared_);
    // reportTimes(s, "write time", numWrites_, writeTiming_, sumWriteTimesSquared_);
    // reportRate(s, "write rate", bytesWritten_, writeTiming_);

    // // Read statistics

    // reportCount(s, "num reads", numReads_);
    // reportBytes(s, "bytes read", numReads_, bytesRead_, sumBytesReadSquared_);
    // reportTimes(s, "read time", numReads_, readTiming_, sumReadTimesSquared_);
    // reportRate(s, "read rate", bytesRead_, readTiming_);

    // // ArchiveCatKvOpen statistics
    // reportCount(s, "num archive catalogue kv open", numArchiveCatKvOpen_);
    // reportTimes(s, "archive catalogue kv open time", numArchiveCatKvOpen_, archiveCatKvOpenTiming_, sumArchiveCatKvOpenTimesSquared_);

    eckit::Timing t;
    size_t numOps = 0;
    double sumTimesSquared = 0;

    for (const auto& x : md_stats_) {

        reportCount(s, x.first, x.second.numOps);
        reportTimes(s, x.first, x.second.numOps, x.second.timing, x.second.sumTimesSquared);
        t += x.second.timing;
        numOps += x.second.numOps;
        sumTimesSquared += x.second.sumTimesSquared;
   
    }

    reportTimes(s, "total DAOS IO time", numOps, t, sumTimesSquared);

}

// void IOStats::print(std::ostream& s) const {
//     s << "IOStats()";
// }

void DaosIOStats::reportCount(std::ostream& s, const std::string& label, size_t num) const {

    s << prefix_ << label << std::setw(FORMAT_WIDTH - label.length()) << " : " << eckit::BigNum(num) << std::endl;

}

// void IOStats::reportBytes(std::ostream& s, const std::string& label, size_t num, size_t sum, size_t sumSquares) const {

//     std::string lbl = label + " (tot, avg, std dev)";

//     double average = 0;
//     double stdDeviation = 0;
//     if (num != 0) {
//         average = sum / num;
//         stdDeviation = std::sqrt(std::max((num * sumSquares) - (sum * sum), size_t(0))) / num;
//     }

//     s << prefix_ << lbl << std::setw(FORMAT_WIDTH - lbl.length()) << " : " << BigNum(sum) << " (" << Bytes(sum) << ")"
//       << ", " << BigNum(size_t(average)) << " (" << Bytes(average) << ")"
//       << ", " << BigNum(size_t(stdDeviation)) << " (" << Bytes(stdDeviation) << ")" << std::endl;
// }

void DaosIOStats::reportTimes(std::ostream& s, const std::string& label, size_t num, const eckit::Timing& sum,
                              double sumSquares) const {

    double elapsed = sum.elapsed_;
    std::string lbl = label + " (tot, avg, std dev)";

    double average = 0;
    double stdDeviation = 0;
    if (num != 0) {
        average = elapsed / num;
        stdDeviation = std::sqrt(std::max((num * sumSquares) - (elapsed * elapsed), 0.0)) / num;
    }

    s << prefix_ << lbl << std::setw(FORMAT_WIDTH - lbl.length()) << " : " << elapsed << "s"
      << ", " << average << "s"
      << ", " << stdDeviation << "s" << std::endl;

}

// void IOStats::reportRate(std::ostream& s, const std::string& label, size_t bytes, const Timing& time) const {

//     double elapsed = time.elapsed_;
//     double rate = 0;

//     if (bytes != 0 && elapsed > 0) {
//         rate = bytes / elapsed;
//     }

//     s << prefix_ << label << std::setw(FORMAT_WIDTH - label.length()) << " : " << Bytes(rate) << " per second"
//       << std::endl;
// }

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5