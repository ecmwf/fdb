/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   October 2018

#ifndef fdb5_FDBAggregateListObject_H
#define fdb5_FDBAggregateListObject_H

#include "fdb5/api/helpers/FDBListObject.h"

#include "eckit/container/Queue.h"

/*
 * Given a function which will push elements onto a queue, run it in
 * the background.
 */

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBAsyncListObject : public FDBListImplBase {

public: // methods

    FDBAsyncListObject(std::function<void(eckit::Queue<FDBListElement>&)> workerFn);
    virtual ~FDBAsyncListObject() override;

    virtual bool next(FDBListElement& elem) override;

private: // members

    eckit::Queue<FDBListElement> queue_;

    std::thread workerThread_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
