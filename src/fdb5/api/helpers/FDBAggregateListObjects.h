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

#include <queue>

/*
 * Define a standard object which can be used to iterate the results of a
 * list() call on an arbitrary FDB object
 */

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBAggregateListObjects : public FDBListImplBase {

public: // methods

    FDBAggregateListObjects(std::queue<FDBListObject>&& listObjects);
    virtual ~FDBAggregateListObjects() override;

    virtual bool next(FDBListElement& elem) override;

private: // members

    std::queue<FDBListObject> listObjects_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
