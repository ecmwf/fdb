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
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/FieldLocation.h"

namespace fdb5 {

FieldLocationFactory::FieldLocationFactory() {
}

FieldLocationFactory& FieldLocationFactory::instance() {
    static FieldLocationFactory theOne;
    return theOne;
}

void FieldLocationFactory::add(const std::string& name, FieldLocationBuilderBase* builder) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    if(has(name)) {
        throw eckit::SeriousBug("Duplicate entry in FieldLocationFactory: " + name, Here());
    }
    builders_[name] = builder;
}

void FieldLocationFactory::remove(const std::string& name) {
    builders_.erase(name);
}

bool FieldLocationFactory::has(const std::string& name) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    return builders_.find(name) != builders_.end();
}

void FieldLocationFactory::list(std::ostream& out) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    const char* sep = "";
    for (std::map<std::string, FieldLocationBuilderBase*>::const_iterator j = builders_.begin(); j != builders_.end(); ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}

FieldLocation* FieldLocationFactory::build(const std::string& name, const eckit::URI &uri) {

    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    auto j = builders_.find(name);

    eckit::Log::debug() << "Looking for FieldLocationBuilder [" << name << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No FieldLocationBuilder for [" << name << "]" << std::endl;
        eckit::Log::error() << "FieldLocationBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No FieldLocationBuilder called ") + name);
    }

    return (*j).second->make(uri);
}

FieldLocation* FieldLocationFactory::build(const std::string& name, const eckit::URI &uri, eckit::Offset offset, eckit::Length length) {

    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    auto j = builders_.find(name);

    eckit::Log::debug() << "Looking for FieldLocationBuilder [" << name << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No FieldLocationBuilder for [" << name << "]" << std::endl;
        eckit::Log::error() << "FieldLocationBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No FieldLocationBuilder called ") + name);
    }

    return (*j).second->make(uri, offset, length);
}

//----------------------------------------------------------------------------------------------------------------------

FieldLocationBuilderBase::FieldLocationBuilderBase(const std::string& name) : name_(name) {
    FieldLocationFactory::instance().add(name_, this);
}

FieldLocationBuilderBase::~FieldLocationBuilderBase() {
    FieldLocationFactory::instance().remove(name_);
}

//----------------------------------------------------------------------------------------------------------------------


/*FieldLocation::FieldLocation() {
}*/

FieldLocation::FieldLocation(const eckit::URI &uri, const eckit::Offset &offset, const eckit::Length &length) : uri_(uri) {
    long long l = length;
    long long o = offset;
    uri_.query("length", std::to_string(l));
    uri_.fragment(std::to_string(o));
    //uri_.query("length", eckit::Translator<eckit::Length, std::string>()(length));
    //uri_.fragment(eckit::Translator<eckit::Offset, std::string>()(offset));
}

FieldLocation::FieldLocation(const eckit::URI &uri) :
    uri_(uri) {}

FieldLocation::FieldLocation(eckit::Stream& s) {
    std::string uri;
    s >> uri;
    uri_ = eckit::URI(uri);
}

eckit::Offset FieldLocation::offset() const {
    return eckit::Offset(std::stoll(uri_.fragment()));
//    return eckit::Translator<std::string, eckit::Offset>()(uri_.fragment());
}

eckit::Length FieldLocation::length() const {
    return eckit::Length(std::stoll(uri_.query("length")));
//    return eckit::Translator<std::string, eckit::Length>()(uri_.query("length"));
}

void FieldLocation::encode(eckit::Stream& s) const {
    s << uri_;
}

void FieldLocation::dump(std::ostream& out) const {
    out << "  uri: " << uri_.asRawString();
}

void FieldLocation::print(std::ostream& out) const {
    out << "  FieldLocation[uri=" << uri_.asRawString() << "]";
}

//----------------------------------------------------------------------------------------------------------------------

FieldLocationVisitor::~FieldLocationVisitor()
{}

void FieldLocationPrinter::operator()(const FieldLocation& location) {
    location.dump(out_);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
