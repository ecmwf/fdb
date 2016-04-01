/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   FdbTask.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_FdbTask_H
#define fdb_FdbTask_H

#include "eckit/memory/NonCopyable.h"

#include "marskit/MarsRequest.h"

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

class FdbTask : public eckit::NonCopyable {

public: // methods

    FdbTask(const marskit::MarsRequest& request);

    ~FdbTask();

    const marskit::MarsRequest& request() const { return request_; }

    void notifyWinds() const;

private: // members

    marskit::MarsRequest request_;

    mutable bool windNotified_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif
