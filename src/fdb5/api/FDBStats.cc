/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/log/Timer.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Bytes.h"

#include "fdb5/api/FDBStats.h"
#include "fdb5/LibFdb.h"


using namespace eckit;

namespace fdb5 {

FDBStats::FDBStats() :
    numArchive_(0),
    numFlush_(0),
    numRetrieve_(0),
    bytesArchive_(0),
    bytesRetrieve_(0),
    sumBytesArchiveSquared_(0),
    sumBytesRetrieveSquared_(0),
    elapsedArchive_(0),
    elapsedFlush_(0),
    elapsedRetrieve_(0),
    sumArchiveTimingSquared_(0),
    sumRetrieveTimingSquared_(0),
    sumFlushTimingSquared_(0) {}


FDBStats::~FDBStats() {}


FDBStats& FDBStats::operator+=(const FDBStats& rhs) {
    numArchive_ += rhs.numArchive_;
    numFlush_ += rhs.numFlush_;
    numRetrieve_ += rhs.numRetrieve_;
    bytesArchive_ += rhs.bytesArchive_;
    bytesRetrieve_ += rhs.bytesRetrieve_;
    sumBytesArchiveSquared_ += rhs.sumBytesArchiveSquared_;
    sumBytesRetrieveSquared_ += rhs.sumBytesRetrieveSquared_;
    elapsedArchive_ += rhs.elapsedArchive_;
    elapsedFlush_ += rhs.elapsedFlush_;
    elapsedRetrieve_ += rhs.elapsedRetrieve_;
    sumArchiveTimingSquared_ += rhs.sumArchiveTimingSquared_;
    sumRetrieveTimingSquared_ += rhs.sumRetrieveTimingSquared_;
    sumFlushTimingSquared_ += rhs.sumFlushTimingSquared_;
}


void FDBStats::addArchive(size_t length, eckit::Timer& timer) {

    numArchive_++;
    bytesArchive_ += length;
    sumBytesArchiveSquared_ += length * length;

    double elapsed = timer.elapsed();
    elapsedArchive_ += elapsed;
    sumArchiveTimingSquared_ += elapsed * elapsed;

    Log::debug<LibFdb>() << "Archive count: " << numArchive_
                         << ", size: " << Bytes(length)
                         << ", total: " << Bytes(bytesArchive_)
                         << ", time: " << Seconds(elapsed)
                         << ", total: " << Seconds(elapsedArchive_) << std::endl;
}


void FDBStats::addRetrieve(size_t length, eckit::Timer& timer) {

    numRetrieve_++;
    bytesRetrieve_ += length;
    sumBytesRetrieveSquared_ += length * length;

    double elapsed = timer.elapsed();
    elapsedRetrieve_ += elapsed;
    sumRetrieveTimingSquared_ += elapsed * elapsed;

    Log::debug<LibFdb>() << "Retrieve count: " << numRetrieve_
                         << ", size: " << Bytes(length)
                         << ", total: " << Bytes(bytesRetrieve_)
                         << ", time: " << Seconds(elapsed)
                         << ", total: " << Seconds(elapsedRetrieve_) << std::endl;
}


void FDBStats::addFlush(eckit::Timer& timer) {

    numFlush_++;

    double elapsed = timer.elapsed();
    elapsedFlush_ += elapsed;
    sumFlushTimingSquared_ += elapsed * elapsed;

    Log::debug<LibFdb>() << "Flush count: " << numFlush_
                         << ", time: " << elapsed << "s"
                         << ", total: " << elapsedFlush_ << "s" << std::endl;

}


void FDBStats::report(std::ostream& out, const char* prefix) const {

    // Archive statistics

    reportCount(out, "num archive", numArchive_, prefix);
    reportBytesStats(out, "bytes archived", numArchive_, bytesArchive_, sumBytesArchiveSquared_, prefix);
    reportTimeStats(out, "archive time", numArchive_, elapsedArchive_, sumArchiveTimingSquared_, prefix);
    reportRate(out, "archive rate", bytesArchive_, elapsedArchive_, prefix);

    // Retrieve statistics

    reportCount(out, "num retrieve", numRetrieve_, prefix);
    reportBytesStats(out, "bytes retrieved", numRetrieve_, bytesRetrieve_, sumBytesRetrieveSquared_, prefix);
    reportTimeStats(out, "retrieve time", numRetrieve_, elapsedRetrieve_, sumRetrieveTimingSquared_, prefix);
    reportRate(out, "retrieve rate", bytesRetrieve_, elapsedRetrieve_, prefix);

    // Flush statistics

    reportCount(out, "num flush", numFlush_, prefix);
    reportTimeStats(out, "flush time", numFlush_, elapsedFlush_, sumFlushTimingSquared_, prefix);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fbdb5
