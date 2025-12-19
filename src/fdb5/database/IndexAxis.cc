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
#include "eckit/log/Log.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/AxisRegistry.h"
#include "fdb5/database/IndexAxis.h"

#include <memory>
#include "fdb5/database/Key.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IndexAxis::IndexAxis() : readOnly_(false), dirty_(false) {}

IndexAxis::~IndexAxis() {
    if (!readOnly_) {
        return;
    }

    for (AxisMap::iterator it = axis_.begin(); it != axis_.end(); ++it) {
        AxisRegistry::instance().release(it->first, it->second);
    }
}

IndexAxis::IndexAxis(eckit::Stream& s, const int version) : readOnly_(true), dirty_(false) {

    decode(s, version);
}

IndexAxis::IndexAxis(IndexAxis&& rhs) noexcept :
    axis_(std::move(rhs.axis_)), readOnly_(rhs.readOnly_), dirty_(rhs.dirty_) {}

IndexAxis& IndexAxis::operator=(IndexAxis&& rhs) noexcept {
    axis_ = std::move(rhs.axis_);
    readOnly_ = rhs.readOnly_;
    dirty_ = rhs.dirty_;
    return *this;
}

bool IndexAxis::operator==(const IndexAxis& rhs) const {

    if (axis_.size() != rhs.axis_.size()) {
        return false;
    }

    for (const auto& kv : axis_) {
        auto it = rhs.axis_.find(kv.first);
        if (it == rhs.axis_.end()) {
            return false;
        }
        if (*kv.second != *it->second) {
            return false;
        }
    }

    return true;
}

bool IndexAxis::operator!=(const IndexAxis& rhs) const {
    return !(*this == rhs);
}

size_t encodeString(size_t len) {
    return (5 + len);
}

size_t IndexAxis::encodeSize(const int version) const {
    size_t size = 2;
    size += encodeString(4) + 5;
    size += encodeString(4);
    for (const auto& [key, vals] : axis_) {
        size += encodeString(key.length()) + 5;
        for (const auto& v : *vals) {
            size += encodeString(v.length());
        }
    }
    return size;
}

void IndexAxis::encode(eckit::Stream& s, const int version) const {
    if (version >= 3) {
        encodeCurrent(s, version);
    }
    else {
        encodeLegacy(s, version);
    }
}

void IndexAxis::encodeCurrent(eckit::Stream& s, const int version) const {
    ASSERT(version >= 3);

    s.startObject();
    s << "size" << axis_.size();
    s << "axes";
    for (AxisMap::const_iterator i = axis_.begin(); i != axis_.end(); ++i) {
        s << (*i).first;
        const eckit::DenseSet<std::string>& values = *(*i).second;
        s << values.size();
        for (eckit::DenseSet<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {
            s << (*j);
        }
    }
    s.endObject();
}

void IndexAxis::encodeLegacy(eckit::Stream& s, const int version) const {
    ASSERT(version <= 2);

    s << axis_.size();
    for (AxisMap::const_iterator i = axis_.begin(); i != axis_.end(); ++i) {
        s << (*i).first;
        const eckit::DenseSet<std::string>& values = *(*i).second;
        s << values.size();
        for (eckit::DenseSet<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {
            s << (*j);
        }
    }
}


void IndexAxis::decode(eckit::Stream& s, const int version) {
    if (version >= 3) {
        decodeCurrent(s, version);
    }
    else {
        decodeLegacy(s, version);
    }
}

enum IndexAxisStreamKeys {
    IndexAxisKeyUnrecognised,
    IndexAxisSize,
    IndexAxes
};

IndexAxisStreamKeys indexAxiskeyId(const std::string& s) {
    static const std::map<std::string, IndexAxisStreamKeys> keys{
        {"size", IndexAxisSize},
        {"axes", IndexAxes},
    };

    auto it = keys.find(s);
    if (it != keys.end()) {
        return it->second;
    }
    return IndexAxisKeyUnrecognised;
}

void IndexAxis::decodeCurrent(eckit::Stream& s, const int version) {
    ASSERT(version >= 3);

    ASSERT(s.next());
    ASSERT(axis_.empty());

    std::string k;
    std::string v;
    size_t n = 0;
    while (!s.endObjectFound()) {
        s >> k;
        switch (indexAxiskeyId(k)) {
            case IndexAxisSize:
                s >> n;
                break;
            case IndexAxes:
                ASSERT(n);
                for (size_t i = 0; i < n; i++) {
                    s >> k;
                    std::shared_ptr<eckit::DenseSet<std::string>>& values = axis_[k];
                    values.reset(new eckit::DenseSet<std::string>);
                    size_t m;
                    s >> m;
                    for (size_t j = 0; j < m; j++) {
                        s >> v;
                        values->insert(v);
                    }
                    values->sort();
                    AxisRegistry::instance().deduplicate(k, values);
                }
                break;
            default:
                throw eckit::SeriousBug("IndexBase de-serialization error: " + k + " field is not recognized");
        }
    }
    ASSERT(!axis_.empty());
}

void IndexAxis::decodeLegacy(eckit::Stream& s, const int version) {

    ASSERT(version <= 2);

    size_t n;
    s >> n;

    std::string k;
    std::string v;

    for (size_t i = 0; i < n; i++) {
        s >> k;
        std::shared_ptr<eckit::DenseSet<std::string>>& values = axis_[k];
        values.reset(new eckit::DenseSet<std::string>);
        size_t m;
        s >> m;
        for (size_t j = 0; j < m; j++) {
            s >> v;
            values->insert(v);
        }
        values->sort();
        AxisRegistry::instance().deduplicate(k, values);
    }
}

void IndexAxis::dump(std::ostream& out, const char* indent) const {
    out << indent << "Axes:" << std::endl;
    for (AxisMap::const_iterator i = axis_.begin(); i != axis_.end(); ++i) {
        out << indent << indent << (*i).first << std::endl;
        const eckit::DenseSet<std::string>& values = *(*i).second;
        for (eckit::DenseSet<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {
            out << indent << indent << indent;
            if ((*j).empty()) {
                out << "<empty>";
            }
            else {
                out << (*j);
            }
            out << std::endl;
        }
    }
    // out << std::endl;
}

bool IndexAxis::partialMatch(const metkit::mars::MarsRequest& request) const {

    // We partially match on a request
    //
    // --> keys that are in the request, but not the axis are OK (other parts of the request)
    // --> keys that are in the axis, but not the request are OK (list doesn't need to specify everything)
    //
    // BUT keys that correspond to the axis object, but do not match it, should result
    // in the match failing (this will be the common outcome during the model run, when many
    // indexes exist)

    auto matchValues = [](const std::vector<std::string>& rqValues, const eckit::DenseSet<std::string>& values) {
        if (rqValues.empty()) {
            return true;
        }
        for (const auto& rqval : rqValues) {
            if (values.contains(rqval)) {
                return true;
            }
        }
        return false;
    };

    for (const auto& [keyword, values] : axis_) {
        if (!matchValues(request.values(keyword, true), *values)) {
            return false;
        }
    }

    return true;
}

bool IndexAxis::contains(const Key& key) const {
    for (const auto& [keyword, values] : axis_) {
        if (!key.matchValues(keyword, *values)) {
            return false;
        }
    }
    return true;
}

bool IndexAxis::containsPartial(const Key& key) const {
    for (const auto& kv : key) {
        auto it = axis_.find(kv.first);
        if (it == axis_.end()) {
            return false;
        }
        else {
            if (!it->second->contains(kv.second)) {
                return false;
            }
        }
    }
    return true;
}

void IndexAxis::insert(const Key& key) {
    ASSERT(!readOnly_);

    for (const auto& [keyword, value] : key) {

        auto& axis_set = axis_[keyword];

        if (!axis_set) {
            axis_set = std::make_shared<eckit::DenseSet<std::string>>();
        }

        axis_set->insert(value);

        dirty_ = true;
    }
}

/// @note: this method inserts key-value pairs into an axis in memory.
///   Intended for importing axis information from storage in the DAOS backend.
///   Input values are required to be cannoicalised.
void IndexAxis::insert(const std::string& axis, const std::vector<std::string>& values) {
    ASSERT(!readOnly_);

    std::shared_ptr<eckit::DenseSet<std::string>>& axis_set = axis_[axis];

    if (!axis_set) {
        axis_set.reset(new eckit::DenseSet<std::string>());
    }

    for (const auto& value : values) {
        axis_set->insert(value);
    }

    dirty_ = true;
}

bool IndexAxis::dirty() const {
    return dirty_;
}


void IndexAxis::clean() {
    dirty_ = false;
}

void IndexAxis::sort() {
    for (AxisMap::iterator i = axis_.begin(); i != axis_.end(); ++i) {
        i->second->sort();
    }
}

void IndexAxis::wipe() {

    ASSERT(!readOnly_);

    axis_.clear();
    clean();
}

bool IndexAxis::has(const std::string& keyword) const {
    AxisMap::const_iterator i = axis_.find(keyword);
    return (i != axis_.end());
}

const eckit::DenseSet<std::string>& IndexAxis::values(const std::string& keyword) const {

    // If an Index is empty, this is bad, but is not strictly an error. Nothing will
    // be found...

    if (axis_.empty()) {
        eckit::Log::warning() << "Querying axis of empty Index: " << keyword << std::endl;
        const static eckit::DenseSet<std::string> nullStringSet;
        return nullStringSet;
    }

    AxisMap::const_iterator i = axis_.find(keyword);
    if (i == axis_.end()) {
        throw eckit::SeriousBug("Cannot find Axis: " + keyword);
    }
    return *(i->second);
}

std::map<std::string, eckit::DenseSet<std::string>> IndexAxis::map() const {

    // Make a copy of the axis map
    std::map<std::string, eckit::DenseSet<std::string>> result;

    for (const auto& kv : axis_) {
        result.emplace(kv.first, *kv.second);
    }
    return result;
}

void IndexAxis::print(std::ostream& out) const {
    out << "IndexAxis["
        << "axis=";

    const char* sep = "";
    out << "{";
    for (const auto& kv : axis_) {
        out << sep << kv.first << "=(";
        const char* sep2 = "";
        for (const auto& v : *kv.second) {
            out << sep2 << v;
            sep2 = ",";
        }
        out << ")";
        sep = ",";
    }
    out << "}";
    out << "]";
}

void IndexAxis::json(eckit::JSON& json) const {
    json.startObject();
    for (const auto& kv : axis_) {
        json << kv.first << *kv.second;
    }
    json.endObject();
}

void IndexAxis::merge(const fdb5::IndexAxis& other) {

    ASSERT(!readOnly_);
    for (const auto& kv : other.axis_) {

        auto it = axis_.find(kv.first);
        if (it == axis_.end()) {
            /// @note: Have to make a copy, otherwise we risk modifying cached axes in the AxisRegistry.
            axis_.emplace(kv.first, std::make_shared<eckit::DenseSet<std::string>>(*kv.second));
        }
        else {
            it->second->merge(*kv.second);
        };
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
