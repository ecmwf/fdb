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

#include <map>
#include <functional>

#include "eckit/log/Statistics.h"

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
        start(label, fn);
    }
    void start(const std::string& label, std::function<void(const std::string&, eckit::Timer&)> fn) {
        label_ = label;
        fun_ = fn;
        timer_.stop();
        timer_.start();
        stopped_ = false;
    }
    void stop() {
        if (stopped_) return;
        timer_.stop();
        fun_(std::move(label_), timer_);
        stopped_ = true;
    }
    ~StatsTimer() {
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

    void logMdOperation(const std::string&, eckit::Timer&);
    // void logDtxOperation(const std::string&, const eckit::Length& size, eckit::Timer&);

private:  // methods
    // void print(std::ostream& s) const;

    void reportCount(std::ostream& s, const std::string& label, size_t num) const;
    // void reportBytes(std::ostream& s, const std::string& label, size_t num, size_t sum, size_t sumSquares) const;
    void reportTimes(std::ostream& s, const std::string& label, size_t num, const eckit::Timing& sum,
                     double sumSquares) const;
    // void reportRate(std::ostream& s, const std::string& label, size_t bytes, const eckit::Timing& time) const;

private:  // members

    std::string prefix_;

    std::map<std::string, MetadataOperationStats> md_stats_;
    std::map<std::string, DataTransferOperationStats> tx_stats_;

// private:  // methods
//     friend std::ostream& operator<<(std::ostream& s, const IOStats& p) {
//         p.print(s);
//         return s;
//     }

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
