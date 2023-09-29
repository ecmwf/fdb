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

#pragma once

// #include <string>
#include <map>

#include "eckit/log/Statistics.h"
// #include "eckit/memory/NonCopyable.h"
// #include "eckit/log/Timer.h"

// namespace eckit {
// class Length;
// class Timer;
// }  // namespace eckit

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class StatsTimer {
    std::string label_;
    eckit::Timer& timer_;
    std::function<void(const std::string&, eckit::Timer&)> fun_;
    bool stopped_;

public:
    explicit StatsTimer(const std::string& label, eckit::Timer& t, std::function<void(const std::string&, eckit::Timer&)> fn) : 
        label_(label), timer_(t), fun_(fn), stopped_(false) {
        timer_.start();
    }
    void start(const std::string& label, std::function<void(const std::string&, eckit::Timer&)> fn) {
        label_ = label;
        fun_ = fn;
        timer_.start();
        stopped_ = false;
    }
    void stop() {
        timer_.stop();
        fun_(std::move(label_), timer_);
        stopped_ = true;
    }
    ~StatsTimer() {
        if (stopped_) return;
        stop();
    }
};

struct MetadataOperationStats {
    size_t numOps = 0;
    eckit::Timing timing;
    double sumTimesSquared = 0;
};

struct DataTransferOperationStats {
    size_t numOps = 0;
    size_t bytes = 0;
    size_t sumBytesSquared = 0;
    eckit::Timing timing;
    double sumTimesSquared = 0;
};

class DaosIOStats : public eckit::NonCopyable {

public:

    DaosIOStats(const std::string& prefix = std::string());
    ~DaosIOStats();

    void report(std::ostream& s) const;

    // DaosCatalogueWriter::selectIndex
    // void logArcCatKvOpen(eckit::Timer& timer);
    // void logArcCatKvCheck(eckit::Timer& timer);
    // void logArcIdxKvCreate(eckit::Timer& timer);
    // void logArcIdxKvPutKey(eckit::Timer& timer);
    // void logArcIdxKvPutIndex(eckit::Timer& timer);
    // void logArcCatKvGetIndex(eckit::Timer& timer);
    // void logArcCatKvClose(eckit::Timer& timer);

    void logMdOperation(const std::string&, eckit::Timer&);
    // void logDtxOperation(const std::string&, const eckit::Length& size, eckit::Timer&);

    // void logRead(const eckit::Length& size, eckit::Timer& timer);
    // void logWrite(const eckit::Length& size, eckit::Timer& timer);
    // void logFlush(eckit::Timer& timer);

private:  // methods
    // void print(std::ostream& s) const;

    void reportCount(std::ostream& s, const std::string& label, size_t num) const;
    // void reportBytes(std::ostream& s, const std::string& label, size_t num, size_t sum, size_t sumSquares) const;
    void reportTimes(std::ostream& s, const std::string& label, size_t num, const eckit::Timing& sum,
                     double sumSquares) const;
    // void reportRate(std::ostream& s, const std::string& label, size_t bytes, const eckit::Timing& time) const;

private:  // members

    std::string prefix_;

//     // The data elements that we actually want to track

//     size_t numReads_;
//     size_t bytesRead_;
//     size_t sumBytesReadSquared_;
//     eckit::Timing readTiming_;
//     double sumReadTimesSquared_;

//     size_t numWrites_;
//     size_t bytesWritten_;
//     size_t sumBytesWrittenSquared_;
//     eckit::Timing writeTiming_;
//     double sumWriteTimesSquared_;

    std::map<std::string, MetadataOperationStats> md_stats_;
    std::map<std::string, DataTransferOperationStats> tx_stats_;

    // size_t numArcCatKvOpen_;
    // eckit::Timing arcCatKvOpenTiming_;
    // double sumArcCatKvOpenTimesSquared_;
    
    // size_t numArcCatKvCheck_;
    // eckit::Timing arcCatKvCheckTiming_;
    // double sumArcCatKvCheckTimesSquared_;
    
    // size_t numArcIdxKvCreate_;
    // eckit::Timing arcIdxKvCreateTiming_;
    // double sumArcIdxKvCreateTimesSquared_;
    
    // size_t numArcIdxKvPutKey_;
    // eckit::Timing arcIdxKvPutKeyTiming_;
    // double sumArcIdxKvPutKeyTimesSquared_;
    
    // size_t numArcCatKvPutIndex_;
    // eckit::Timing arcCatKvPutIndexTiming_;
    // double sumArcCatKvPutIndexTimesSquared_;
    
    // size_t numArcCatKvGetIndex_;
    // eckit::Timing arcCatKvGetIndexTiming_;
    // double sumArcCatKvGetIndexTimesSquared_;

    // size_t numArcCatKvClose_;
    // eckit::Timing arcCatKvClose_;
    // double sumArcCatKvCloseTimesSquared_;

// private:  // methods
//     friend std::ostream& operator<<(std::ostream& s, const IOStats& p) {
//         p.print(s);
//         return s;
//     }

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5