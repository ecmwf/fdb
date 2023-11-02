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
/// @date Nov 2023

#include <cmath>
#include <iomanip>

#include "eckit/log/BigNum.h"

#include "fdb5/toc/TocIOStats.h"

static const int FORMAT_WIDTH = 75;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocIOStats::TocIOStats(const std::string& prefix) :
    prefix_(prefix) {

    if (!prefix_.empty())
        prefix_ += std::string(" ");

}

TocIOStats::~TocIOStats() {}

void TocIOStats::logMdOperation(const std::string& label, eckit::Timer& timer) {

    std::map<std::string, MetadataOperationStats>::iterator it = md_stats_.find(label);

    if (it == md_stats_.end()) {
        it = md_stats_.insert(std::make_pair(std::move(label), MetadataOperationStats())).first;
    }

    it->second.numOps++;
    it->second.timing += timer;

    double elapsed = timer.elapsed();
    it->second.sumTimesSquared += elapsed * elapsed;

}

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

void TocIOStats::report(std::ostream& s) const {

    // // Read statistics

    // reportCount(s, "num reads", numReads_);
    // reportBytes(s, "bytes read", numReads_, bytesRead_, sumBytesReadSquared_);
    // reportTimes(s, "read time", numReads_, readTiming_, sumReadTimesSquared_);
    // reportRate(s, "read rate", bytesRead_, readTiming_);

    eckit::Timing t;
    size_t numOps = 0;
    double sumTimesSquared = 0;

    eckit::Timing t_daos;
    size_t numOps_daos = 0;
    double sumTimesSquared_daos = 0;

    for (const auto& x : md_stats_) {

        reportCount(s, x.first, x.second.numOps);
        reportTimes(s, x.first, x.second.numOps, x.second.timing, x.second.sumTimesSquared);

        if (x.first.rfind("misc ", 0) == 0) continue;

        if (x.first.rfind("posix_", 0) == 0) {
            t_daos += x.second.timing;
            numOps_daos += x.second.numOps;
            sumTimesSquared_daos += x.second.sumTimesSquared;
        } else {
            t += x.second.timing;
            numOps += x.second.numOps;
            sumTimesSquared += x.second.sumTimesSquared;
        }
   
    }

    reportTimes(s, "total POSIX API IO time", numOps_daos, t_daos, sumTimesSquared_daos);
    reportTimes(s, "total profiled FDB/TOC IO time", numOps, t, sumTimesSquared);

}

// void IOStats::print(std::ostream& s) const {
//     s << "IOStats()";
// }

void TocIOStats::reportCount(std::ostream& s, const std::string& label, size_t num) const {

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

void TocIOStats::reportTimes(std::ostream& s, const std::string& label, size_t num, const eckit::Timing& sum,
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
