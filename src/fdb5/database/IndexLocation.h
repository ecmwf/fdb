// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0


#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/serialisation/Streamable.h"

#pragma once

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class IndexLocation : public eckit::Streamable {

public:  // methods

    IndexLocation();
    ~IndexLocation() override;

    //    virtual eckit::PathName path() const = 0;
    virtual eckit::URI uri() const = 0;

    virtual IndexLocation* clone() const = 0;

private:  // methods

    virtual void print(std::ostream& out) const = 0;


protected:  // For streamable

    void encode(eckit::Stream&) const override = 0;

    static eckit::ClassSpec classSpec_;

private:  // friends

    friend std::ostream& operator<<(std::ostream& s, const IndexLocation& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

class IndexLocationVisitor {

public:  // methods

    virtual ~IndexLocationVisitor();

    virtual void operator()(const IndexLocation&) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

