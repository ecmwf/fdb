// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/exception/Exceptions.h"

#include "fdb5/database/DataStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class NullDataStats : public DataStatsContent {
public:

    virtual void add(const DataStatsContent&) { NOTIMP; }

    virtual void report(std::ostream& out, const char* indent) const { NOTIMP; }
};

//----------------------------------------------------------------------------------------------------------------------

DataStats::DataStats() : content_(new NullDataStats()) {
    content_->attach();
}

DataStats::DataStats(DataStatsContent* p) : content_(p) {
    content_->attach();
}

DataStats::~DataStats() {
    content_->detach();
}

DataStats::DataStats(const DataStats& s) : content_(s.content_) {
    content_->attach();
}

DataStats& DataStats::operator=(const DataStats& s) {
    content_->detach();
    content_ = s.content_;
    content_->attach();
    return *this;
}

void DataStats::add(const DataStats& s) {
    content_->add(*s.content_);
}

void DataStats::report(std::ostream& out, const char* indent) const {
    content_->report(out, indent);
}

DataStatsContent::~DataStatsContent() {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
