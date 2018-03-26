/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/database/Key.h"

#include "eckit/parser/Tokenizer.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDB::FDB(const Config &config) :
    internal_(FDBFactory::build(config)),
    dirty_(false) {

    // Select operates as constraints of the form: class=regex,key=regex,...
    // By default, there is no select.

    std::string select = config.getString("select", "");

    std::vector<std::string> select_key_values;
    eckit::Tokenizer(',')(select, select_key_values);

    eckit::Tokenizer equalsTokenizer('=');
    for (const std::string& key_value : select_key_values) {
        std::vector<std::string> kv;
        equalsTokenizer(key_value, kv);

        if (kv.size() != 2 || select_.find(kv[0]) != select_.end()) {
            std::stringstream ss;
            ss << "Invalid select condition for pool: " << select << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }

        select_[kv[0]] = eckit::Regex(kv[1]);
    }
}


FDB::~FDB() {}

void FDB::archive(const Key& key, const void* data, size_t length) {
    ASSERT(matches(key));
    internal_->archive(key, data, length);
    dirty_ = true;
}

eckit::DataHandle *FDB::retrieve(const MarsRequest& request) {
    // TODO: Match?
    return internal_->retrieve(request);
}

const std::string FDB::id() const {
    return internal_->id();
}

bool FDB::matches(const Key& key) {

    for (const auto& kv : select_) {
        const std::string& k(kv.first);
        const eckit::Regex& re(kv.second);

        eckit::StringDict::const_iterator i = key.find(k);
        if (i == key.end() || !re.match(i->second)) return false;
    }

    return true;
}

void FDB::print(std::ostream& s) const {
    s << *internal_;
}

void FDB::flush() {
    if (dirty_) {
        internal_->flush();
        dirty_ = false;
    }
}

bool FDB::dirty() const {
    return dirty_;
}

void FDB::setNonWritable() {
    return internal_->setNonWritable();
}

bool FDB::writable() const {
    return internal_->writable();
}

bool FDB::visitable() const {
    return internal_->visitable();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
