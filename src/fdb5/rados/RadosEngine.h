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

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/rados/RadosKeyValue.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/Engine.h"
#include "fdb5/fdb5_config.h"

#include <optional>
#include <ostream>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RadosEngine : public fdb5::Engine {

public:  // methods

    RadosEngine() {};

    static const char* typeName() { return "rados"; }

protected:  // methods

    std::string name() const override;

    std::string dbType() const override { NOTIMP; };

    eckit::URI location(const Key& key, const Config& config) const override { NOTIMP; };

    bool canHandle(const eckit::URI&, const Config&) const override { NOTIMP; };

    // std::vector<eckit::URI> allLocations(const Key& key, const Config& config) const override { NOTIMP; };

    std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const override;
    std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq,
                                               const Config& config) const override;

    // std::vector<eckit::URI> writableLocations(const Key& key, const Config& config) const override { NOTIMP; };

    void print(std::ostream& out) const override { NOTIMP; };

private:  // methods

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    void readConfig(const fdb5::Config& config, const std::string& component, bool readPool) const;
#else
    void readConfig(const fdb5::Config& config, const std::string& component, bool readNamespace) const;
#endif

protected:  // members

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
    mutable std::optional<eckit::RadosAsyncKeyValue> root_kv_;
    // std::optional<eckit::RadosAsyncKeyValue> db_kv_;
#else
    mutable std::optional<eckit::RadosKeyValue> root_kv_;
    // std::optional<eckit::RadosKeyValue> db_kv_;
#endif

    // eckit::Length maxPartSize_;

private:  // members

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    mutable std::string nspace_prefix_;
#else
    mutable std::string pool_prefix_;
#endif
};

//----------------------------------------------------------------------------------------------------------------------


}  // namespace fdb5
