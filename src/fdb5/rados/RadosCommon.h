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
/// @date   Feb 2024

#pragma once

#include "eckit/filesystem/URI.h"
#include "eckit/utils/Optional.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosPersistentKeyValue.h"

#include "fdb5/fdb5_config.h"
#include "fdb5/database/Key.h"
#include "fdb5/config/Config.h"

namespace fdb5 {

class RadosCommon {

public: // methods

    RadosCommon(const fdb5::Config&, const std::string& component, const fdb5::Key&);
    RadosCommon(const fdb5::Config&, const std::string& component, const eckit::URI&);

private: // methods

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    void readConfig(const fdb5::Config& config, const std::string& component, bool readPool);
#else
    void readConfig(const fdb5::Config& config, const std::string& component, bool readNamespace);
#endif

protected: // members

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    std::string pool_;
    std::string root_namespace_;
    std::string db_namespace_;
#else
    std::string root_pool_;
    std::string db_pool_;
    std::string namespace_;
#endif

#if defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE) || defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
    eckit::Optional<eckit::RadosPersistentKeyValue> root_kv_;
    eckit::Optional<eckit::RadosPersistentKeyValue> db_kv_;
#else
    eckit::Optional<eckit::RadosKeyValue> root_kv_;
    eckit::Optional<eckit::RadosKeyValue> db_kv_;
#endif

    eckit::Length maxObjectSize_;

#ifndef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
private: // members

    std::string prefix_;
#endif

};

}


