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

/// @author Simon Smart
/// @date   Mar 2018

#ifndef fdb5_api_FDBFactory_H
#define fdb5_api_FDBFactory_H

#include <memory>

#include "eckit/distributed/Transport.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/utils/Regex.h"

#include "fdb5/api/FDBStats.h"
#include "fdb5/api/helpers/AxesIterator.h"
#include "fdb5/api/helpers/Callback.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/DumpIterator.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"

namespace eckit::message {

class Message;

}  // namespace eckit::message

namespace metkit {

class MarsRequest;

}  // namespace metkit

namespace fdb5 {

class Key;
class FDBToolRequest;

//----------------------------------------------------------------------------------------------------------------------

/// The base class that FDB implementations are derived from

class FDBBase : private eckit::NonCopyable, public CallbackRegistry {

public:  // methods

    FDBBase(const Config& config, const std::string& name);
    virtual ~FDBBase();

    // -------------- Primary API functions ----------------------------

    virtual void archive(const Key& key, const void* data, size_t length) = 0;

    virtual void reindex(const Key& key, const FieldLocation& location) { NOTIMP; }

    virtual void flush() = 0;

    virtual ListIterator inspect(const metkit::mars::MarsRequest& request) = 0;

    virtual ListIterator list(const FDBToolRequest& request, int level) = 0;

    virtual DumpIterator dump(const FDBToolRequest& request, bool simple) = 0;

    virtual StatusIterator status(const FDBToolRequest& request) = 0;

    virtual WipeIterator wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) = 0;

    virtual PurgeIterator purge(const FDBToolRequest& request, bool doit, bool porcelain) = 0;

    virtual StatsIterator stats(const FDBToolRequest& request) = 0;

    virtual ControlIterator control(const FDBToolRequest& request, ControlAction action,
                                    ControlIdentifiers identifier) = 0;

    virtual MoveIterator move(const FDBToolRequest& request, const eckit::URI& dest) = 0;

    virtual AxesIterator axesIterator(const FDBToolRequest& request, int axes) = 0;

    // -------------- API management ----------------------------

    /// ID used for hashing in the Rendezvous hash. Should be unique amongst those used
    /// within a DistFDB (i.e. within one Rendezvous hash).
    virtual std::string id() const;

    const std::string& name() const;

    const Config& config() const;

    bool enabled(const ControlIdentifier& controlIdentifier) const;

private:  // methods

    virtual void print(std::ostream& s) const = 0;

    friend std::ostream& operator<<(std::ostream& s, const FDBBase& f) {
        f.print(s);
        return s;
    }

protected:  // members

    const std::string name_;

    Config config_;

    ControlIdentifiers controlIdentifiers_;
};

//----------------------------------------------------------------------------------------------------------------------

class FDBBuilderBase;

class FDBFactory {
public:

    static FDBFactory& instance();

    void add(const std::string& name, const FDBBuilderBase*);

    std::unique_ptr<FDBBase> build(const Config& config);

private:

    FDBFactory() {}  ///< private constructor only used by singleton

    eckit::Mutex mutex_;

    std::map<std::string, const FDBBuilderBase*> registry_;
};


class FDBBuilderBase {
public:  // methods

    virtual std::unique_ptr<FDBBase> make(const Config& config) const = 0;

protected:  // methods

    FDBBuilderBase(const std::string& name);

    virtual ~FDBBuilderBase();

protected:  // members

    std::string name_;
};


template <typename T>
class FDBBuilder : public FDBBuilderBase {

    static_assert(std::is_base_of<FDBBase, T>::value,
                  "FDB Factorys can only build implementations of the FDB interface");

public:  // methods

    FDBBuilder(const std::string& name) : FDBBuilderBase(name) {}

private:  // methods

    std::unique_ptr<FDBBase> make(const Config& config) const override {
        return std::unique_ptr<T>(new T(config, name_));
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_api_FDBFactory_H
