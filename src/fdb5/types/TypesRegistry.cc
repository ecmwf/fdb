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

#include "fdb5/types/TypesRegistry.h"
#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypesRegistry::TypesRegistry():
    parent_(nullptr) {
}

TypesRegistry::~TypesRegistry() {
    for (TypeMap::iterator i = cache_.begin(); i != cache_.end(); ++i) {
        delete (*i).second;
    }
}

void TypesRegistry::updateParent(std::shared_ptr<TypesRegistry> parent) {
    parent_ = parent;
}


void TypesRegistry::addType(const std::string &keyword, const std::string &type) {
    ASSERT(types_.find(keyword) == types_.end());
    types_[keyword] = type;
}

const Type &TypesRegistry::lookupType(const std::string &keyword) const {
    std::map<std::string, Type *>::const_iterator j = cache_.find(keyword);

    if (j != cache_.end()) {
        return *(*j).second;
    } else {

        std::string type = "Default";
        std::map<std::string, std::string>::const_iterator i = types_.find(keyword);
        if (i != types_.end()) {
            type = (*i).second;
        } else {
            if (parent_) {
                return parent_->lookupType(keyword);
            }
        }

        // eckit::Log::info() << "Type of " << keyword << " is " << type << std::endl;
        Type* newKH = TypesFactory::build(type, keyword);
        cache_[keyword] = newKH;
        return *newKH;
    }
}

std::ostream &operator<<(std::ostream &s, const TypesRegistry &x) {
    x.print(s);
    return s;
}

void TypesRegistry::print( std::ostream &out ) const {
    out << this << "(" << types_ << ")";
}

void TypesRegistry::dump( std::ostream &out ) const {
    for (std::map<std::string, std::string>::const_iterator i = types_.begin(); i != types_.end(); ++i) {
        out << i->first << ":" << i->second << ";" << std::endl;
    }
}


void TypesRegistry::dump( std::ostream &out, const std::string &keyword ) const {
    std::map<std::string, std::string>::const_iterator i = types_.find(keyword);

    out << keyword;
    if (i != types_.end()) {
        out << ":" << i->second;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
