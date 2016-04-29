/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   MasterConfig.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_MasterConfig_H
#define fdb5_MasterConfig_H

#include "eckit/memory/NonCopyable.h"

#include "fdb5/DB.h"
#include "fdb5/Schema.h"
#include "fdb5/TypesRegistry.h"

class MarsTask;
class MarsRequest;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MasterConfig : public eckit::NonCopyable {

public: // methods

    static MasterConfig &instance();

    const Schema &schema() const;

    std::string schemaPath() const;


private: // methods

    MasterConfig();

    /// Destructor

    ~MasterConfig();


private: // members

    Schema schema_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
