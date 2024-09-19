/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"

#include "fdb5/database/IndexStats.h"

namespace fdb5 {

::eckit::ClassSpec IndexStatsContent::classSpec_ = {
    &Streamable::classSpec(),
    "IndexStatsContent",
};

//----------------------------------------------------------------------------------------------------------------------

class NullIndexStats : public IndexStatsContent {
public:

    virtual size_t fieldsCount() const { return 0; }
    virtual size_t duplicatesCount() const { return 0; }

    virtual size_t fieldsSize() const { return 0; }
    virtual size_t duplicatesSize() const { return 0; }

    virtual size_t addFieldsCount(size_t i) { return i; }
    virtual size_t addDuplicatesCount(size_t i) { return i; }

    virtual size_t addFieldsSize(size_t i) { return i; }
    virtual size_t addDuplicatesSize(size_t i) { return i; }

    virtual void add(const IndexStatsContent&) { NOTIMP; }

    virtual void report(std::ostream& out, const char* indent) const { NOTIMP; }

    virtual void encode(eckit::Stream& s) const { NOTIMP; }
};

//----------------------------------------------------------------------------------------------------------------------


IndexStats::IndexStats() : content_(new NullIndexStats()) {
    content_->attach();
}

IndexStats::IndexStats(IndexStatsContent* p) : content_(p) {
    content_->attach();
}

IndexStats::~IndexStats() {
    content_->detach();
}

IndexStats::IndexStats(const IndexStats& s) : content_(s.content_) {
    content_->attach();
}

IndexStats& IndexStats::operator=(const IndexStats& s) {
    content_->detach();
    content_ = s.content_;
    content_->attach();
    return *this;
}

IndexStats& IndexStats::operator+=(const IndexStats& s) {
    add(s);
    return *this;
}

void IndexStats::add(const IndexStats& s) {
    content_->add(*s.content_);
}

void IndexStats::report(std::ostream& out, const char* indent) const {
    content_->report(out, indent);
}

void IndexStats::encode(eckit::Stream& s) const {
    s << *content_;
}

IndexStatsContent::~IndexStatsContent() {}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
