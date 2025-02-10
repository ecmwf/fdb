/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   HandleGatherer.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_HandleGatherer_H
#define fdb5_HandleGatherer_H

#include <cstdlib>
#include <iosfwd>
#include <vector>

#include "eckit/memory/NonCopyable.h"

namespace eckit {
class DataHandle;
}


namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------

class HandleGatherer : public eckit::NonCopyable {

public:  // methods

    HandleGatherer(bool sorted);

    ~HandleGatherer();

    void add(eckit::DataHandle*);

    eckit::DataHandle* dataHandle();

    size_t count() const;


private:  // members

    bool sorted_;
    std::vector<eckit::DataHandle*> handles_;
    size_t count_;

    void print(std::ostream& out) const;
    friend std::ostream& operator<<(std::ostream& s, const HandleGatherer& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
