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
#include "fdb5/Rules.h"
#include "fdb5/Handlers.h"

class MarsTask;
class MarsRequest;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MasterConfig : public eckit::NonCopyable {

public: // methods

    static MasterConfig& instance();

    eckit::SharedPtr<DB> openSessionDB(const Key& userkey);

    Key makeDBKey(const Key& key) const;

    const Rules& rules() const;

    const KeywordHandler& lookupHandler(const std::string& keyword) const;

private: // methods

    MasterConfig();

    /// Destructor

    ~MasterConfig();

    void expand(const MarsRequest& request,
                size_t pos,
                Key& dbKey,
                VecDB& result );

private: // members

    Rules rules_;
    Handlers handlers_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
