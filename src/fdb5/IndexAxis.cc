/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "eckit/io/FileHandle.h"
#include "eckit/value/Value.h"
#include "eckit/value/Content.h"

#include "eckit/parser/JSON.h"

#include "fdb5/IndexAxis.h"
#include "fdb5/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IndexAxis::IndexAxis() :
    readOnly_(false) {
}

IndexAxis::~IndexAxis() {
}

void IndexAxis::load(const eckit::Value &v) {

    eckit::ValueMap m = v.as<eckit::ValueMap>();

    for ( eckit::ValueMap::iterator i = m.begin(); i != m.end(); ++i ) {
        std::string k( i->first );

        std::set<std::string> &s = axis_[i->first];

        eckit::ValueList list = i->second.as<eckit::ValueList>();

        for ( eckit::ValueList::iterator j = list.begin(); j != list.end(); ++j ) {
            s.insert( std::string(*j) );
        }
    }

    readOnly_ = true;
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
        << "path=" << path_
        <<  ",axis=";
    eckit::__print_container(out, axis_);
    out  << "]";
}

void IndexAxis::json(eckit::JSON &j) const {
    j << axis_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
