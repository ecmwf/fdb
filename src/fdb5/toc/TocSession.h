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

#pragma once

#include "eckit/runtime/Main.h"
#include "eckit/utils/Translator.h"

#include "eckit/log/Timer.h"

#include "fdb5/toc/TocIOStats.h"

#include "fdb5/fdb5_config.h"

namespace fdb5 {

class TocManager : private eckit::NonCopyable {

public: // methods

    static TocManager& instance() {
        static TocManager instance;
        return instance;
    };

    fdb5::TocIOStats& stats() { return stats_; }
    eckit::Timer& timer() { return timer_; }
    eckit::Timer& fsCallTimer() { return fs_call_timer_; }
    eckit::Timer& miscTimer() { return misc_timer_; }

private: // methods

    TocManager() : stats_(std::string("FDB TOC profiling ") + eckit::Main::hostname() + ":" + eckit::Translator<int, std::string>()(::getpid())) {}

private: // members

    /// @todo: it is the FDB class who should own the DaosIOStats and pass on to the Catalogue and Store
    fdb5::TocIOStats stats_;
    eckit::Timer timer_;
    eckit::Timer fs_call_timer_;
    eckit::Timer misc_timer_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
