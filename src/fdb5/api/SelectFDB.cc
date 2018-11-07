/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/types/Types.h"

#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/SelectFDB.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/LibFdb.h"

#include "marslib/MarsTask.h"


namespace fdb5 {

static FDBBuilder<SelectFDB> selectFdbBuilder("select");

//----------------------------------------------------------------------------------------------------------------------


std::map<std::string, eckit::Regex> parseFDBSelect(const eckit::LocalConfiguration& config) {

    std::map<std::string, eckit::Regex> selectDict;

    // Select operates as constraints of the form: class=regex,key=regex,...
    // By default, there is no select.

    std::string select = config.getString("select", "");

    std::vector<std::string> select_key_values;
    eckit::Tokenizer(',')(select, select_key_values);

    eckit::Tokenizer equalsTokenizer('=');
    for (const std::string& key_value : select_key_values) {
        std::vector<std::string> kv;
        equalsTokenizer(key_value, kv);

        if (kv.size() != 2 || selectDict.find(kv[0]) != selectDict.end()) {
            std::stringstream ss;
            ss << "Invalid select condition for pool: " << select << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }

        selectDict[kv[0]] = eckit::Regex(kv[1]);
    }

    return selectDict;
}


SelectFDB::SelectFDB(const Config& config, const std::string& name) :
    FDBBase(config, name) {

    ASSERT(config.getString("type", "") == "select");

    if (!config.has("fdbs")) {
        throw eckit::UserError("fdbs not specified for select FDB", Here());
    }

    std::vector<eckit::LocalConfiguration> fdbs(config.getSubConfigurations("fdbs"));
    for (const eckit::LocalConfiguration& c : fdbs) {
        subFdbs_.push_back(FDB(c));
        selects_.emplace_back(parseFDBSelect(c));
    }
}


SelectFDB::~SelectFDB() {}


void SelectFDB::archive(const Key& key, const void* data, size_t length) {

    ASSERT(subFdbs_.size() == selects_.size());

    for (size_t idx = 0; idx < subFdbs_.size(); idx++) {

        bool matches = true;
        for (const auto& kv : selects_[idx]) {
            const std::string& k(kv.first);
            const eckit::Regex& re(kv.second);

            eckit::StringDict::const_iterator i = key.find(k);
            if (i == key.end() || !re.match(i->second)) {
                matches = false;
                break;
            }
        }

        if (matches) {
            subFdbs_[idx].archive(key, data, length);
            return;
        }
    }

    std::stringstream ss;
    ss << "No matching fdb for key: " << key;
    throw eckit::UserError(ss.str(), Here());
}


eckit::DataHandle *SelectFDB::retrieve(const MarsRequest& request) {

    // TODO: Select within the SubFDBs

//    HandleGatherer result(true); // Sorted
    HandleGatherer result(false);

    for (FDB& fdb : subFdbs_) {
//        if (subFdbs_.matches()) {
        result.add(fdb.retrieve(request));
//        }
    }

    return result.dataHandle();
}

ListIterator SelectFDB::list(const FDBToolRequest &request) {

    // TODO: Matching FDBs?

    std::queue<ListIterator> lists;

    for (FDB& fdb : subFdbs_) {
        lists.push(fdb.list(request));
    }

    return ListIterator(new ListAggregateIterator(std::move(lists)));
}

std::string SelectFDB::id() const {
    NOTIMP;
}


void SelectFDB::flush() {
    for (FDB& fdb : subFdbs_) {
        fdb.flush();
    }
}


void SelectFDB::print(std::ostream &s) const {
    s << "SelectFDB()";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
