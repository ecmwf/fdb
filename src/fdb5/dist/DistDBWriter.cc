/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_config.h"

#include "eckit/log/Log.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/LibFdb.h"

#include "fdb5/dist/DistDBWriter.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/ArchiveVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// TODO: Discuss, do we want to use the Archiver, or do we want to directly open the sub DB?


DistDBWriter::DistDBWriter(const Key &key, const eckit::Configuration& config) :
    DistDB(key, config) {

    // Construct a list of DBs to write to

    std::vector<eckit::PathName> lanes;
    manager_.writableLanes(key, lanes);

    if (lanes.size() == 0) {
        std::stringstream ss;
        ss << "No matching, writable lanes for key: " << key;
        throw eckit::UserError(ss.str(), Here());
    }

    for (size_t i = 0; i < lanes.size(); i++) {
        eckit::LocalConfiguration sub_config;
        sub_config.set("fdb_home", lanes[i]);
        eckit::Log::debug<LibFdb>() << "Opening lane: " << lanes[i] << std::endl;
        archivers_.push_back(new Archiver(sub_config));
    }
}


DistDBWriter::DistDBWriter(const eckit::PathName &directory, const eckit::Configuration& config) :
    DistDB(directory, config) {}


DistDBWriter::~DistDBWriter() {

    while (archivers_.size()) {
        delete archivers_.back();
        archivers_.pop_back();
    }
}

void DistDBWriter::print(std::ostream& out) const {
    out << "DistDBWriter()";
}

bool DistDBWriter::selectIndex(const Key &key) {
    eckit::Log::debug<LibFdb>() << "Dist DB Writer select index: " << key << std::endl;
    indexKey_ = key;
}

void DistDBWriter::deselectIndex() {
    indexKey_.clear();
}

void DistDBWriter::checkSchema(const Key &key) const {

    // TODO: If we directly use the sub DBs, then we can pass this through...
    //       (and enforce the same schema in all sub DBs?)
}

void DistDBWriter::archive(const Key &key, const void *data, Length length) {

    eckit::Log::debug<LibFdb>() << "DistD DB Writer archive key: " << key << std::endl;

    // TODO: This is horribly inefficienct, given we started with the Key
    // TODO: It is also horribly inefficient that we walk the full schema multiple times...
    //       Can we justforward the selectIndex and archive calls...

    Key fullKey = dbKey_;
    for (Key::const_iterator it = indexKey_.begin(); it != indexKey_.end(); ++it) {
        fullKey.set(it->first, it->second);
    }
    for (Key::const_iterator it = key.begin(); it != key.end(); ++it) {
        fullKey.set(it->first, it->second);
    }

    for (size_t i = 0; i < archivers_.size(); i++) {
        ArchiveVisitor visitor(*archivers_[i], fullKey, data, length);
        archivers_[i]->archive(fullKey, visitor);
    }
}

void DistDBWriter::flush() {
    for (size_t i = 0; i < archivers_.size(); i++) {
        archivers_[i]->flush();
    }
}



static DBBuilder<DistDBWriter> builder("dist.writer", false, true);

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
