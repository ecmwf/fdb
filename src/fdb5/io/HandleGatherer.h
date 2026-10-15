// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   HandleGatherer.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <cstdlib>
#include <iosfwd>
#include <vector>

namespace eckit {
class DataHandle;
}


namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------

class HandleGatherer {

public:  // methods

    HandleGatherer(bool sorted);

    HandleGatherer(const HandleGatherer&) = delete;
    HandleGatherer& operator=(const HandleGatherer&) = delete;
    HandleGatherer(HandleGatherer&&) = delete;
    HandleGatherer& operator=(HandleGatherer&&) = delete;

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

