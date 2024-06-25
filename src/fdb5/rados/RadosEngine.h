/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Jun 2024

#pragma once

#include "eckit/utils/Optional.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosAsyncKeyValue.h"

#include "fdb5/database/Engine.h"
#include "fdb5/fdb5_config.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RadosEngine : public fdb5::Engine {

public: // methods

    RadosEngine() {};

    static const char* typeName() { return "rados"; }

protected: // methods

    virtual std::string name() const override;

    virtual std::string dbType() const override { NOTIMP; };

    virtual eckit::URI location(const Key &key, const Config& config) const override { NOTIMP; };

    virtual bool canHandle(const eckit::URI&, const Config&) const override { NOTIMP; };

    virtual std::vector<eckit::URI> allLocations(const Key& key, const Config& config) const override { NOTIMP; };

    virtual std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const override;
    virtual std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq, const Config& config) const override;

    virtual std::vector<eckit::URI> writableLocations(const Key& key, const Config& config) const override { NOTIMP; };

    virtual void print( std::ostream &out ) const override { NOTIMP; };

private: // methods

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    void readConfig(const fdb5::Config& config, const std::string& component, bool readPool) const;
#else
    void readConfig(const fdb5::Config& config, const std::string& component, bool readNamespace) const;
#endif

protected: // members

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    mutable std::string pool_;
    mutable std::string root_namespace_;
    // std::string db_namespace_;
#else
    mutable std::string root_pool_;
    // std::string db_pool_;
    mutable std::string namespace_;
#endif

#if defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
    mutable eckit::Optional<eckit::RadosAsyncKeyValue> root_kv_;
    // eckit::Optional<eckit::RadosAsyncKeyValue> db_kv_;
#else
    mutable eckit::Optional<eckit::RadosKeyValue> root_kv_;
    // eckit::Optional<eckit::RadosKeyValue> db_kv_;
#endif

    // eckit::Length maxObjectSize_;

private: // members

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    mutable std::string nspace_prefix_;
#else
    mutable std::string pool_prefix_;
#endif

};

//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5
