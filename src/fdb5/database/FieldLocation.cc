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

::eckit::ClassSpec FieldLocation::classSpec_ = {
    &Streamable::classSpec(),
    "FieldLocation",
};

FieldLocationFactory::FieldLocationFactory() {}

FieldLocationFactory& FieldLocationFactory::instance() {
    static FieldLocationFactory theOne;
    return theOne;
}

void FieldLocationFactory::add(const std::string& name, FieldLocationBuilderBase* builder) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    if (has(name)) {
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
    for (std::map<std::string, FieldLocationBuilderBase*>::const_iterator j = builders_.begin(); j != builders_.end();
         ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}

FieldLocation* FieldLocationFactory::build(const std::string& name, const eckit::URI& uri, eckit::Offset offset,
                                           eckit::Length length, const Key& remapKey) {

    ASSERT(static_cast<long long>(length) != 0ll);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    auto j = builders_.find(name);

    LOG_DEBUG_LIB(LibFdb5) << "Looking for FieldLocationBuilder [" << name << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No FieldLocationBuilder for [" << name << "]" << std::endl;
        eckit::Log::error() << "FieldLocationBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j) {
            eckit::Log::error() << "   " << (*j).first << std::endl;
        }
        throw eckit::SeriousBug(std::string("No FieldLocationBuilder called ") + name);
    }

    return (*j).second->make(uri, offset, length, remapKey);
}

FieldLocation* FieldLocationFactory::build(const std::string& name, const eckit::URI& uri) {

    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    auto j = builders_.find(name);

    LOG_DEBUG_LIB(LibFdb5) << "Looking for FieldLocationBuilder [" << name << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No FieldLocationBuilder for [" << name << "]" << std::endl;
        eckit::Log::error() << "FieldLocationBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j) {
            eckit::Log::error() << "   " << (*j).first << std::endl;
        }
        throw eckit::SeriousBug(std::string("No FieldLocationBuilder called ") + name);
    }

    return (*j).second->make(uri);
}

//----------------------------------------------------------------------------------------------------------------------

FieldLocationBuilderBase::FieldLocationBuilderBase(const std::string& name) : name_(name) {
    FieldLocationFactory::instance().add(name_, this);
}

FieldLocationBuilderBase::~FieldLocationBuilderBase() {
    if (LibFdb5::instance().dontDeregisterFactories()) {
        return;
    }
    FieldLocationFactory::instance().remove(name_);
}

//----------------------------------------------------------------------------------------------------------------------

FieldLocation::FieldLocation(const eckit::URI& uri) : uri_(uri) {
    try {
        offset_ = eckit::Offset(std::stoll(uri.fragment()));
    }
    catch (std::invalid_argument& e) {
        offset_ = eckit::Offset(0);
    }

    std::string lengthStr = uri.query("length");
    if (!lengthStr.empty()) {
        try {
            length_ = eckit::Length(std::stoll(lengthStr));
        }
        catch (std::invalid_argument& e) {
            length_ = eckit::Length(0);
        }
    }
    else {
        length_ = eckit::Length(0);
    }

    const std::string keyStr = uri.query("remapKey");
    if (!keyStr.empty()) {
        remapKey_ = Key::parse(keyStr);
    }
    else {
        remapKey_ = Key();
    }
}

eckit::URI FieldLocation::fullUri() const {
    eckit::URI full = uri_;
    full.fragment(std::to_string(offset_));
    full.query("length", std::to_string(length_));
    if (!remapKey_.empty()) {
        full.query("remapKey", remapKey_);
    }
    return full;
}


FieldLocation::FieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length, const Key& remapKey) :
    uri_(uri), offset_(offset), length_(length), remapKey_(remapKey) {}

void FieldLocation::encode(eckit::Stream& s) const {
    s << uri_;
    s << offset_;
    s << length_;
    s << remapKey_;
}

FieldLocation::FieldLocation(eckit::Stream& s) {
    s >> uri_;
    s >> offset_;
    s >> length_;
    s >> remapKey_;
}

// void FieldLocation::remapKey(const Key& key) { NOTIMP; }
// const Key& FieldLocation::remapKey() const { NOTIMP; }

void FieldLocation::dump(std::ostream& out) const {
    out << "  uri: " << uri().asRawString();
}

void FieldLocation::print(std::ostream& out) const {
    out << "  FieldLocation[uri=" << uri_ << ",offset=" << offset() << ",length=" << length()
        << ",remapKey=" << remapKey_ << "]";
}

//----------------------------------------------------------------------------------------------------------------------

FieldLocationVisitor::~FieldLocationVisitor() {}

void FieldLocationPrinter::operator()(const FieldLocation& location) {
    location.dump(out_);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
