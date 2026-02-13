/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "Types.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"

#include <sstream>

namespace compare {

//---------------------------------------------------------------------------------------------------------------------

std::ostream& operator<<(std::ostream& os, const KeyDiffMap& km) {
    os << "{";
    for (const auto& [k, v] : km) {
        os << k << "=" << "(" << (v.first ? *v.first : "<MISSING>") << ", " << (v.second ? *v.second : "<MISSING>")
           << ")" << ", ";
    }
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const KeySet& km) {
    os << "{";
    for (const auto& k : km) {
        os << k << ", ";
    }
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const DataIndex& idx) {
    for (const auto& [km, loc] : idx) {
        os << "Key: " << km << " -> Value: " << loc << "\n";
    }
    return os;
}

//---------------------------------------------------------------------------------------------------------------------

void parseKeySet(KeySet& container, const std::string& keyStr) {
    std::istringstream stream(keyStr);
    std::string entry;

    while (std::getline(stream, entry, ',')) {
        container.insert(entry);  // Insert the whole entry as a single string
    }
}

KeySet parseKeySet(const std::string& keyStr) {
    KeySet container;
    parseKeySet(container, keyStr);
    return container;
}

compare::KeyDiffMap requestDiff(const fdb5::Key& l, const fdb5::Key& r) {
    compare::KeyDiffMap res;
    for (const auto& [lk, lv] : l) {
        auto [search, isValid] = r.find(lk);
        if (isValid) {
            if (search->second != lv) {
                res.insert({lk, {lv, search->second}});
            }
        }
        else {
            res.insert({lk, {lv, {}}});
        }
    }

    for (const auto& [rk, rv] : r) {
        auto [search, isValid] = l.find(rk);
        if (!isValid) {
            res.insert({rk, {{}, rv}});
        }
    }
    return res;
}

//---------------------------------------------------------------------------------------------------------------------


fdb5::Key applyKeyDiff(fdb5::Key k, const KeyDiffMap& diff, bool swapDiff) {
    for (const auto& [field, val_pair] : diff) {
        const auto& val = swapDiff ? val_pair.first : val_pair.second;
        if (val) {
            k.set(field, *val);
        }
        else {
            // Delete
            k.unset(field);
        }
    }
    return k;  // return modified copy
};

Scope parseScope(const std::string& s) {
    if (s == "header-only") {
        return Scope::HeaderOnly;
    }
    else if (s == "all") {
        return Scope::All;
    }
    else if (s == "mars") {
        return Scope::Mars;
    }
    throw eckit::UserError("Unknown comparison scope " + s, Here());
}
std::ostream& operator<<(std::ostream& os, const Scope& scope) {
    switch (scope) {
        case Scope::HeaderOnly:
            os << "header-only";
            break;
        case Scope::All:
            os << "all";
            break;
        case Scope::Mars:
            os << "mars";
            break;
    }
    return os;
}

Method parseMethod(const std::string& s) {
    if (s == "bit-identical") {
        return Method::BitIdentical;
    }
    else if (s == "hash-keys") {
        return Method::Hash;
    }
    else if (s == "grib-keys") {
        return Method::KeyByKey;
    }
    throw eckit::UserError("Unknown method " + s, Here());
}
std::ostream& operator<<(std::ostream& os, const Method& method) {
    switch (method) {
        case Method::BitIdentical:
            os << "bit-identical";
            break;
        case Method::Hash:
            os << "hash-keys";
            break;
        case Method::KeyByKey:
            os << "grib-keys";
            break;
    }
    return os;
}

//---------------------------------------------------------------------------------------------------------------------

std::ostream& operator<<(std::ostream& os, const NumericError& err) {
    os << "  Average: \t" << err.avg() << std::endl;
    os << "  Min: \t\t" << err.min << std::endl;
    os << "  Max: \t\t" << err.max << std::endl;
    os << "  Count: \t" << err.count << std::endl;
    return os;
}

std::ostream& operator<<(std::ostream& os, const Result& res) {
    os << (res.match ? "SUCCESS" : "FAILURE") << std::endl;
    if (res.relativeError) {
        os << std::endl;
        os << "Relative Error:" << std::endl;
        os << *res.relativeError << std::endl;
    }
    if (res.absoluteError) {
        os << std::endl;
        os << "Absolute Error:" << std::endl;
        os << *res.absoluteError << std::endl;
    }
    return os;
}

//---------------------------------------------------------------------------------------------------------------------


double NumericError::avg() const {
    if (count == 0) {
        return 0.0;
    }
    return sum / count;
}

void NumericError::update(const NumericError& other) {
    this->sum += other.sum;
    this->min = std::min(this->min, other.min);
    this->max = std::max(this->max, other.max);
    this->count += other.count;
}

void NumericError::update(double val) {
    this->sum += val;
    this->min = std::min(this->min, val);
    this->max = std::max(this->max, val);
    this->count += 1;
}

void Result::update(const Result& other) {
    if (this->relativeError) {
        if (other.relativeError) {
            this->relativeError->update(*other.relativeError);
        }
    }
    else {
        this->relativeError = other.relativeError;
    }

    if (this->absoluteError) {
        if (other.absoluteError) {
            this->absoluteError->update(*other.absoluteError);
        }
    }
    else {
        this->absoluteError = other.absoluteError;
    }

    this->match = this->match && other.match;
}

//---------------------------------------------------------------------------------------------------------------------


bool isSubset(const fdb5::Key& a, const fdb5::Key& b) {
    for (const auto& kv : a) {
        auto [it, isValid] = b.find(kv.first);
        if (!isValid || it->second != kv.second)
            return false;
    }
    return true;
}


DataIndex assembleCompareMap(fdb5::FDB& fdb, const fdb5::FDBToolRequest& req, const fdb5::Key& ignore) {
    DataIndex out;

    auto list = fdb.list(req);
    fdb5::ListElement elem;

    while (list.next(elem)) {
        fdb5::Key km = elem.combinedKey();
        if (ignore.empty() || !isSubset(ignore, km)) {
            out.emplace(km, elem);
        }
    }
    eckit::Log::info() << "[LOG] FDB request: " << req << " resulted in " << out.size() << " entries.\n";
    return out;
}

//---------------------------------------------------------------------------------------------------------------------


}  // namespace compare
