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

#include "fdb5/database/DbStats.h"

namespace fdb5 {

::eckit::ClassSpec DbStatsContent::classSpec_ = {&Streamable::classSpec(), "DbStatsContent",};

//----------------------------------------------------------------------------------------------------------------------

class NullDbStats : public DbStatsContent {
public:

    virtual void add(const DbStatsContent&) {
        NOTIMP;
    }

    virtual void report(std::ostream& out, const char* indent) const {
        NOTIMP;
    }

    virtual void encode(eckit::Stream& s) const {
        NOTIMP;
    }
};

//----------------------------------------------------------------------------------------------------------------------

DbStats::DbStats() :
    content_(new NullDbStats()) {
    content_->attach();
}

DbStats::DbStats(DbStatsContent* p) :
    content_(p) {
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

void DbStats::add(const DbStats& s)
{
    content_->add(*s.content_);
}

void DbStats::report(std::ostream& out, const char* indent) const
{
    content_->report(out, indent);
}

void DbStats::encode(eckit::Stream& s) const {
    s << *content_;
}

DbStatsContent::~DbStatsContent()
{
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
