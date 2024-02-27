/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/pmem/PMemDBReader.h"
#include "fdb5/pmem/PMemIndex.h"
#include "fdb5/pmem/PMemStats.h"
#include "fdb5/LibFdb5.h"

using namespace eckit;

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------


PMemDBReader::PMemDBReader(const Key &key, const eckit::Configuration& config) :
    PMemDB(key, config) {}


PMemDBReader::PMemDBReader(const eckit::PathName& directory, const eckit::Configuration& config) :
    PMemDB(directory, config) {}


PMemDBReader::~PMemDBReader() {
}


bool PMemDBReader::open() {

    // We don't want to create a DB if it doesn't exist when opening for read.
    if (!exists())
        return false;

    return PMemDB::open();
}


bool PMemDBReader::selectIndex(const Key &key) {

    if (indexes_.find(key) == indexes_.end()) {

        ::pmem::PersistentPtr<PBranchingNode> node = root_->getBranchingNode(key);
        if (!node.null())
            indexes_[key] = new PMemIndex(key, *node, *dataPoolMgr_);
        else
            return false;
    }

    currentIndex_ = indexes_[key];

    LOG_DEBUG_LIB(LibFdb5) << "PMemDBReader::selectIndex " << key
                                << ", found match" << std::endl;

    return true;
}


void PMemDBReader::axis(const std::string& keyword, eckit::StringSet& s) const {
    s = currentIndex_.axes().values(keyword);
}


eckit::DataHandle* PMemDBReader::retrieve(const Key &key) const {

    LOG_DEBUG_LIB(LibFdb5) << "Trying to retrieve key " << key << std::endl;
    LOG_DEBUG_LIB(LibFdb5) << "From index " << currentIndex_ << std::endl;

    Field field;
    if (currentIndex_.get(key, field))
        return field.dataHandle();

    return 0;
}

std::vector<Index> PMemDBReader::indexes(bool sorted) const {
    NOTIMP;
}

StatsReportVisitor *PMemDBReader::statsReportVisitor() {
    return new PMemStatsReportVisitor(*this);
}

void PMemDBReader::print(std::ostream &out) const {
    out << "PMemDBReader["
        /// @todo should print more here
        << "]";
}

static DBBuilder<PMemDBReader> builder("pmem.reader", true, false);

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
