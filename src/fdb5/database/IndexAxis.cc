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
#include "eckit/exception/Exceptions.h"

#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IndexAxis::IndexAxis() :
    readOnly_(false),
    dirty_(false) {
}

IndexAxis::~IndexAxis() {
}

IndexAxis::IndexAxis(eckit::Stream &s) :
    readOnly_(true),
    dirty_(false) {

    decode(s);
}

void IndexAxis::encode(eckit::Stream &s) const {
    s << axis_.size();
    for (AxisMap::const_iterator i = axis_.begin(); i != axis_.end(); ++i) {
        s << (*i).first;
        const std::set<std::string> &values = (*i).second;
        s << values.size();
        for (std::set<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {
            s << (*j);
        }
    }
}

void IndexAxis::decode(eckit::Stream &s) {

    size_t n;
    s >> n;

    std::string k;
    std::string v;

    for (size_t i = 0; i < n; i++) {
        s >> k;
        std::set<std::string> &values = axis_[k];
        size_t m;
        s >> m;
        for (size_t j = 0; j < m; j++) {
            s >> v;
            values.insert(v);
        }
    }
}

void IndexAxis::dump(std::ostream &out, const char* indent) const {
    out << indent << "Axes:" << std::endl;
   for (AxisMap::const_iterator i = axis_.begin(); i != axis_.end(); ++i) {
        out << indent << indent << (*i).first << std::endl;
        const std::set<std::string> &values = (*i).second;
        for (std::set<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {
            out << indent << indent << indent;
            if ((*j).empty()) {
                out << "<empty>";
            }
            else {
                out << (*j);
            }
            out  << std::endl;
        }
    }
   // out << std::endl;
}

bool IndexAxis::contains(const Key &key) const {

    for (AxisMap::const_iterator i = axis_.begin(); i != axis_.end(); ++i) {
        if (!key.match(i->first, i->second)) {
            return false;
        }
    }
    return true;
}

void IndexAxis::insert(const Key &key) {
    ASSERT(!readOnly_);


    for (Key::const_iterator i = key.begin(); i  != key.end(); ++i) {
        const std::string &keyword = i->first;
        const std::string &value   = i->second;

        std::pair<eckit::StringSet::iterator, bool> result = axis_[keyword].insert(value);
        if (result.second)
            dirty_ = true;
    }
}


bool IndexAxis::dirty() const {
    return dirty_;
}


void IndexAxis::clean() {
    dirty_ = false;
}

void IndexAxis::wipe() {

    ASSERT(!readOnly_);

    axis_.clear();
    clean();
}


const eckit::StringSet &IndexAxis::values(const std::string &keyword) const {

    // If an Index is empty, this is bad, but is not strictly an error. Nothing will
    // be found...

    if (axis_.empty()) {
        eckit::Log::warning() << "Querying axis of empty Index: " << keyword << std::endl;
        const static eckit::StringSet nullStringSet;
        return nullStringSet;
    }

    AxisMap::const_iterator i = axis_.find(keyword);
    if (i == axis_.end()) {
        throw eckit::SeriousBug("Cannot find Axis: " + keyword);
    }
    return i->second;
}

void IndexAxis::print(std::ostream &out) const {
    out << "IndexAxis["
        <<  "axis=";
    eckit::__print_container(out, axis_);
    out  << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
