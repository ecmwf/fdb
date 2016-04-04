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

#ifndef fdb_MasterConfig_H
#define fdb_MasterConfig_H

#include "eckit/memory/NonCopyable.h"

#include "fdb5/DB.h"

class MarsTask;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MasterConfig : public eckit::NonCopyable {

public: // methods

    static MasterConfig& instance();

    eckit::SharedPtr<DB> openSessionDB(const Key& userkey);

    VecDB openSessionDBs(const MarsTask& task);

    bool FailOnOverwrite()   const { return fdbFailOnOverwrite_; }
    bool WarnOnOverwrite()   const { return fdbWarnOnOverwrite_; }
    bool IgnoreOnOverwrite() const { return fdbIgnoreOnOverwrite_; }

private: // methods

	MasterConfig();

    /// Destructor
    
    ~MasterConfig();

    Key makeDBKey(const Key& key) const;


private: // members

    std::vector<std::string> masterDBKeys_;

    bool            fdbFailOnOverwrite_;
    bool            fdbWarnOnOverwrite_;
    bool            fdbIgnoreOnOverwrite_;
    bool            fdbAllKeyChecks_;
    bool            fdbCheckRequired_;
    bool            fdbCheckAcceptable_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
