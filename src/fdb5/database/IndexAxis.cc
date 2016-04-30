/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IndexAxis::IndexAxis() :
    readOnly_(false) {
}

IndexAxis::~IndexAxis() {
}

IndexAxis::IndexAxis(eckit::Stream &s) :
    readOnly_(true) {
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


void IndexAxis::insert(const Key &key) {
    ASSERT(!readOnly_);

    //    Log::info() << *this << std::endl;

    const eckit::StringDict &keymap = key.dict();

    for (eckit::StringDict::const_iterator i = keymap.begin(); i  != keymap.end(); ++i) {
        const std::string &keyword = i->first;
        const std::string &value   = i->second;
        axis_[keyword].insert(value);
    }
}

const eckit::StringSet &IndexAxis::values(const std::string &keyword) const {
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
