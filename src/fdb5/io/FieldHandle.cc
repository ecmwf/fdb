/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <numeric>

#include "eckit/config/Resource.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/log/Timer.h"
#include "eckit/runtime/Metrics.h"
#include "eckit/types/Types.h"

#include "metkit/hypercube/HyperCubePayloaded.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/io/FieldHandle.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FieldHandle::FieldHandle(ListIterator& it) :
    datahandles_({}),
    totalSize_(0),
    currentIdx_(0),
    current_(nullptr),
    currentMemoryHandle_(false),
    buffer_(nullptr),
    sorted_(false),
    seekable_(true) {
    ListElement el;
    eckit::Length largest = 0;

    static bool dedup = eckit::Resource<bool>("fdbDeduplicate;$FDB_DEDUPLICATE_FIELDS", false);
    if (dedup) {
        if (it.next(el)) {
            // build the request representing the tensor-product of all retrieved fields
            metkit::mars::MarsRequest cubeRequest = el.combinedKey().request();
            std::vector<ListElement> elements{el};

            while (it.next(el)) {
                cubeRequest.merge(el.combinedKey().request());
                elements.push_back(el);
            }

            // checking all retrieved fields against the hypercube, to remove duplicates
            ListElementDeduplicator dedup;
            metkit::hypercube::HyperCubePayloaded<ListElement> cube(cubeRequest, dedup);
            for (auto el : elements) {
                cube.add(el.combinedKey().request(), el);
            }

            if (cube.countVacant() > 0) {
                std::stringstream ss;
                ss << "No matching data for requests:" << std::endl;
                for (auto req : cube.vacantRequests()) {
                    ss << "    " << req << std::endl;
                }
                eckit::Log::warning() << ss.str() << std::endl;
            }

            for (size_t i = 0; i < cube.size(); i++) {
                ListElement element;
                if (cube.find(i, element)) {
                    eckit::Length len = element.location().length();
                    eckit::DataHandle* dh = element.location().dataHandle();
                    datahandles_.push_back(std::make_pair(len, dh));

                    totalSize_ += len;
                    bool canSeek = dh->canSeek();
                    if (!canSeek) {
                        largest = std::max(largest, len);
                        seekable_ = false;
                    }
                }
            }
        }
    }
    else {
        while (it.next(el)) {
            eckit::Length len = el.location().length();
            eckit::DataHandle* dh = el.location().dataHandle();
            datahandles_.push_back(std::make_pair(len, dh));

            totalSize_ += len;
            bool canSeek = dh->canSeek();
            if (!canSeek) {
                largest = std::max(largest, len);
                seekable_ = false;
            }
        }
    }

    if (!seekable_) {
        // allocate a buffer that can fit the largest not seekable field (to avoid re-allocations)
        buffer_ = new char[largest];
    }
}

FieldHandle::~FieldHandle() {
    for (size_t i = 0; i < datahandles_.size(); i++) {
        delete datahandles_[i].second;
    }
    if (current_ && currentMemoryHandle_) {
        delete current_;
    }
    delete[] buffer_;
}

void FieldHandle::openCurrent() {

    if (current_ && currentMemoryHandle_) {
        delete current_;
        currentMemoryHandle_ = false;
    }

    if (currentIdx_ < datahandles_.size()) {

        eckit::Length currentSize = datahandles_[currentIdx_].first;
        current_ = datahandles_[currentIdx_].second;
        current_->openForRead();

        if (!current_->canSeek()) {
            long len = 0;
            long toRead = currentSize;
            long read = 0;
            char* buf = buffer_;
            while (toRead > 0 && (len = current_->read(buf, toRead)) > 0) {
                toRead -= len;
                buf += len;
                read += len;
            }

            if (read != static_cast<long>(currentSize)) {
                std::stringstream ss;
                ss << "Error reading from " << *current_ << " - read " << read << ", expected " << currentSize;
                throw eckit::ReadError(ss.str());
            }
            current_ = new eckit::MemoryHandle(buffer_, currentSize);
            current_->openForRead();
            currentMemoryHandle_ = true;
        }
    }
}

eckit::Length FieldHandle::openForRead() {
    ASSERT(!current_);

    currentIdx_ = 0;
    openCurrent();

    return totalSize_;
}

long FieldHandle::read1(char* buffer, long length) {
    if (currentIdx_ >= datahandles_.size()) {
        return 0;
    }

    long n = current_->read(buffer, length);
    if (n <= 0) {
        current_->close();
        currentIdx_++;
        openCurrent();
        return read1(buffer, length);
    }
    return n;
}

long FieldHandle::read(void* buffer, long length) {
    long requested = length;

    char* p = static_cast<char*>(buffer);
    long n = 0;
    long total = 0;

    while (length > 0 && (n = read1(p, length)) > 0) {
        length -= n;
        total += n;
        p += n;
    }

    LOG_DEBUG_LIB(LibFdb5) << "FieldHandle::read - requested: " << requested << "  read: " << (total > 0 ? total : n)
                           << std::endl;

    return total > 0 ? total : n;
}

void FieldHandle::close() {
    if (currentIdx_ < datahandles_.size()) {
        current_->close();
        if (currentMemoryHandle_) {
            delete current_;
        }
        current_ = nullptr;
    }
    currentIdx_ = datahandles_.size();
}

void FieldHandle::rewind() {
    if (currentIdx_ == 0 || seekable_) {
        if (current_ && currentIdx_ < datahandles_.size()) {
            current_->close();
        }
        currentIdx_ = 0;
        openCurrent();
    }
    else {
        throw eckit::ReadError("rewind not supported");
    }
}

void FieldHandle::print(std::ostream& s) const {
    if (eckit::format(s) == eckit::Log::compactFormat) {
        s << "FieldHandle";
    }
    else {
        s << "FieldHandle[";
        for (size_t i = 0; i < datahandles_.size(); i++) {
            if (i != 0) {
                s << ",(";
            }
            datahandles_[i].second->print(s);
            s << ")";
        }
        s << ']';
    }
}

eckit::Length FieldHandle::size() {
    return totalSize_;
}

eckit::Length FieldHandle::estimate() {
    return totalSize_;
}

// only partial seek: within the current message and forward
bool FieldHandle::canSeek() const {
    return true;
}

eckit::Offset FieldHandle::position() {
    long long accumulated = 0;
    for (size_t idx = 0; idx < currentIdx_; idx++) {
        accumulated += datahandles_[idx].first;
    }
    return accumulated + (currentIdx_ >= datahandles_.size() ? eckit::Offset(0) : current_->position());
}

eckit::Offset FieldHandle::seek(const eckit::Offset& offset) {
    if (current_ && currentIdx_ < datahandles_.size()) {
        current_->close();
    }

    const long long seekto = offset;
    long long accumulated = 0;

    if (!seekable_) {  // check that the offset is within the current message of later
        for (size_t idx = 0; idx < currentIdx_; idx++) {
            accumulated += datahandles_[idx].first;
        }

        if (seekto < accumulated) {
            throw eckit::UserError("Cannot seek backward to previous data fields");
        }
    }

    accumulated = 0;
    for (currentIdx_ = 0; currentIdx_ < datahandles_.size(); ++currentIdx_) {
        long long size = datahandles_[currentIdx_].first;
        if (accumulated <= seekto && seekto < accumulated + size) {
            openCurrent();
            current_->seek(seekto - accumulated);
            return offset;
        }
        accumulated += size;
    }
    // check if we went beyond EOF which is POSIX compliant, but we ASSERT so we find possible bugs
    eckit::Offset beyond = seekto - accumulated;
    ASSERT(not beyond);
    return offset;
}

bool FieldHandle::compress(bool) {
    return false;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
