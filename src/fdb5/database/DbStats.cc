// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/exception/Exceptions.h"

#include "fdb5/database/DbStats.h"

namespace fdb5 {

::eckit::ClassSpec DbStatsContent::classSpec_ = {
    &Streamable::classSpec(),
    "DbStatsContent",
};

//----------------------------------------------------------------------------------------------------------------------

class NullDbStats : public DbStatsContent {
public:

    virtual void add(const DbStatsContent&) { NOTIMP; }

    virtual void report(std::ostream& out, const char* indent) const { NOTIMP; }

    void json(eckit::JSON& json) const override { NOTIMP; }

    virtual void encode(eckit::Stream& s) const { NOTIMP; }
};

//----------------------------------------------------------------------------------------------------------------------

DbStats::DbStats() : content_(new NullDbStats()) {
    content_->attach();
}

DbStats::DbStats(DbStatsContent* p) : content_(p) {
    content_->attach();
}

DbStats::~DbStats() {
    content_->detach();
}

DbStats::DbStats(const DbStats& s) : content_(s.content_) {
    content_->attach();
}

DbStats& DbStats::operator=(const DbStats& s) {
    content_->detach();
    content_ = s.content_;
    content_->attach();
    return *this;
}

DbStats& DbStats::operator+=(const DbStats& s) {
    add(s);
    return *this;
}

void DbStats::add(const DbStats& s) {
    content_->add(*s.content_);
}

void DbStats::report(std::ostream& out, const char* indent) const {
    content_->report(out, indent);
}

void DbStats::json(eckit::JSON& json) const {
    content_->json(json);
}

void DbStats::encode(eckit::Stream& s) const {
    s << *content_;
}

DbStatsContent::~DbStatsContent() {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
