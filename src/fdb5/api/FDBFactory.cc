/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */


#include "eckit/config/YAMLConfiguration.h"
#include "eckit/log/Log.h"
#include "eckit/message/Message.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/LibFdb5.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


FDBBase::FDBBase(const Config& config, const std::string& name) :
    name_(name),
    config_(config),
    disabled_(false) {

    bool writable = config.getBool("writable", true);
    bool visitable = config.getBool("visitable", true);
    if (!config.getBool("list", visitable)) {
        controlIdentifiers_ |= ControlIdentifier::List;
    }
    if (!config.getBool("retrieve", visitable)) {
        controlIdentifiers_ |= ControlIdentifier::Retrieve;
    }
    if (!config.getBool("archive", writable)) {
        controlIdentifiers_ |= ControlIdentifier::Archive;
    }
    if (!config.getBool("wipe", writable)) {
        controlIdentifiers_ |= ControlIdentifier::Wipe;
    }

    eckit::Log::debug<LibFdb5>() << "FDBBase: " << config << std::endl;
}


FDBBase::~FDBBase() {}

std::string FDBBase::id() const {
    std::stringstream ss;
    ss << config_;
    return ss.str();
}

FDBStats FDBBase::stats() const {
    /// By default we have no additional internal statistics
    return FDBStats();
}

const std::string &FDBBase::name() const {
    return name_;
}

const Config& FDBBase::config() const {
    return config_;
}

bool FDBBase::enabled(const ControlIdentifier& controlIdentifier) const {
    return controlIdentifiers_.enabled(controlIdentifier);
}

void FDBBase::disable() {
    eckit::Log::warning() << "Disabling FDB " << *this << std::endl;
    disabled_ = true;
}

bool FDBBase::disabled() {
    return disabled_;
}


void FDBBase::move(const FDBToolRequest& request, const eckit::URI& dest) {

    // assert it is locked for archival...
    StatusIterator it = status(request);
    StatusElement elem;
    ASSERT(it.next(elem));
    ASSERT(!elem.controlIdentifiers.enabled(ControlIdentifier::Archive));
    ASSERT(!elem.controlIdentifiers.enabled(ControlIdentifier::Wipe));
    ASSERT(!elem.controlIdentifiers.enabled(ControlIdentifier::UniqueRoot));

    std::unique_ptr<DB> dbSource = DB::buildReader(elem.key, config_);
    if (!dbSource->exists()) {
        std::stringstream ss;
        ss << "Source database not found: " << dbSource << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    if (dbSource->canMoveTo(dest)) {
        dbSource->moveTo(dest);
    }
}

FDBFactory& FDBFactory::instance()
{
    static FDBFactory fdbfactory;
    return fdbfactory;
}

void FDBFactory::add(const std::string& name, const FDBBuilderBase* b)
{
    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    ASSERT(registry_.find(name) == registry_.end());

    registry_[name] = b;
}

std::unique_ptr<FDBBase> FDBFactory::build(const Config& config) {

    // Allow expanding of the config to make use of fdb_home supplied in a previous
    // configuration file, or to pick up the default configuration from ~fdb/etc/fdb/...

    Config actualConfig = config.expandConfig();

    /// We use "local" as a default type if not otherwise configured.

    std::string key = actualConfig.getString("type", "local");

    eckit::Log::debug<LibFdb5>() << "Selecting FDB implementation: " << key << std::endl;

    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    auto it = registry_.find(key);

    if (it == registry_.end()) {
        std::stringstream ss;
        ss << "FDB factory \"" << key << "\" not found";
        throw eckit::SeriousBug(ss.str(), Here());
    }

    std::unique_ptr<FDBBase> ret = it->second->make(actualConfig);
    eckit::Log::debug<LibFdb5>() << "Constructed FDB implementation: " << *ret << std::endl;
    return ret;
}

FDBBuilderBase::FDBBuilderBase(const std::string &name) :
    name_(name) {

    FDBFactory::instance().add(name, this);
}

FDBBuilderBase::~FDBBuilderBase() {
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
