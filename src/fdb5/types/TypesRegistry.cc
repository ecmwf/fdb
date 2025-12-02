/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstddef>
#include <memory>
#include <utility>

#include "eckit/exception/Exceptions.h"

#include "eckit/log/Log.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypesRegistry.h"

#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/Parameter.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec TypesRegistry::classSpec_ = {&eckit::Streamable::classSpec(), "TypesRegistry"};

eckit::Reanimator<TypesRegistry> TypesRegistry::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

TypesRegistry::TypesRegistry(eckit::Stream& stream) {

    decode(stream);
}

void TypesRegistry::decode(eckit::Stream& stream) {

    size_t typeSize = 0;
    stream >> typeSize;
    for (size_t i = 0; i < typeSize; ++i) {
        std::string keyword, type;

        stream >> keyword;
        stream >> type;

        types_[keyword] = type;
    }
}

void TypesRegistry::encode(eckit::Stream& out) const {
    out << types_.size();
    for (const auto& [keyword, type] : types_) {
        out << keyword;
        out << type;
    }
}

std::size_t TypesRegistry::hash() const {
    std::size_t h = 0;

    for (const auto& [keyword, type] : types_) {
        // this hash combine is inspired in the boost::hash_combine
        std::string s = keyword + type;
        h ^= std::hash<std::string>{}(s) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
}

bool TypesRegistry::operator==(const TypesRegistry& other) const {
    return types_ == other.types_;
}

void TypesRegistry::updateParent(const TypesRegistry& parent) {
    parent_ = &parent;
}

void TypesRegistry::addType(const std::string& keyword, const std::string& type) {
    ASSERT(types_.find(keyword) == types_.end());
    types_[keyword] = type;
}

const Type& TypesRegistry::lookupType(const std::string& keyword) const {
    std::lock_guard<std::mutex> lock(cacheMutex_);

    if (auto iter = cache_.find(keyword); iter != cache_.end()) {
        return *iter->second;
    }

    std::string type = "Default";

    if (auto iter = types_.find(keyword); iter != types_.end()) {
        type = iter->second;
    }
    else if (parent_) {
        return parent_->lookupType(keyword);
    }

    auto* newType = TypesFactory::build(type, keyword);

    if (const auto [iter, success] = cache_.try_emplace(keyword, newType); !success) {
        LOG_DEBUG_LIB(LibFdb5) << "Failed to insert new type into cache" << std::endl;
    }

    return *newType;
}

metkit::mars::MarsRequest TypesRegistry::canonicalise(const metkit::mars::MarsRequest& request) const {
    metkit::mars::MarsRequest result(request.verb());

    for (const auto& param : request.parameters()) {
        const std::vector<std::string>& srcVals = param.values();
        std::set<std::string> uniqueVals;
        std::vector<std::string> vals;
        const Type& type = lookupType(param.name());
        for (const std::string& v : srcVals) {
            std::string newVal = type.toKey(v);
            auto it = uniqueVals.find(newVal);
            if (it == uniqueVals.end()) {
                vals.push_back(newVal);
                uniqueVals.insert(newVal);
            }
        }
        result.values(type.alias(), vals);
    }

    return result;
}

void TypesRegistry::print(std::ostream& out) const {
    out << this << "(" << types_ << ")";
}

void TypesRegistry::dump(std::ostream& out) const {
    for (const auto& [keyword, type] : types_) {
        out << keyword << ":" << type << ";" << std::endl;
    }
}

void TypesRegistry::dump(std::ostream& out, const std::string& keyword) const {
    out << keyword;
    if (auto iter = types_.find(keyword); iter != types_.end()) {
        out << ":" << iter->second;
    }
}

std::ostream& operator<<(std::ostream& out, const TypesRegistry& registry) {
    registry.print(out);
    return out;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
