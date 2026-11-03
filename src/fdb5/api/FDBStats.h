// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @author Tiago Quintino
/// @date   June 2018

#pragma once

#include "eckit/log/Statistics.h"

#include <iosfwd>


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBStats : public eckit::Statistics {
public:

    FDBStats();
    ~FDBStats();

    // rules
    FDBStats(const FDBStats&) = default;
    FDBStats(FDBStats&&) = default;
    FDBStats& operator=(const FDBStats&) = default;
    FDBStats& operator=(FDBStats&&) = default;

    size_t numArchive() const { return numArchive_; }
    size_t numLocation() const { return numLocation_; }
    size_t numFlush() const { return numFlush_; }

    void addArchive(size_t length, eckit::Timer& timer, size_t nfields = 1);
    void addLocation(size_t nfields = 1);
    void addRetrieve(size_t length, eckit::Timer& timer);
    void addFlush(eckit::Timer& timer);

    void report(std::ostream& out, const char* indent) const;

    FDBStats& operator+=(const FDBStats& rhs);

private:  // members

    size_t numArchive_;
    size_t numLocation_;
    size_t numFlush_;
    size_t numRetrieve_;

    size_t bytesArchive_;
    size_t bytesRetrieve_;

    size_t sumBytesArchiveSquared_;
    size_t sumBytesRetrieveSquared_;

    double elapsedArchive_;
    double elapsedFlush_;
    double elapsedRetrieve_;

    double sumArchiveTimingSquared_;
    double sumRetrieveTimingSquared_;
    double sumFlushTimingSquared_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
