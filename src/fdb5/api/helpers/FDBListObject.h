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

#ifndef fdb5_FDBListObject_H
#define fdb5_FDBListObject_H

#include "fdb5/database/Key.h"
#include "fdb5/database/FieldLocation.h"

#include <memory>
#include <iosfwd>

/*
 * Define a standard object which can be used to iterate the results of a
 * list() call on an arbitrary FDB object
 */

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBListElement {
public: // methods

    FDBListElement() = default;
    FDBListElement(const std::vector<Key>& keyParts, std::shared_ptr<const FieldLocation> location);

    void print(std::ostream& out, bool location=false) const;

private: // methods

    friend std::ostream& operator<<(std::ostream& os, const FDBListElement& e) {
        e.print(os);
        return os;
    }

public: // members

    std::vector<Key> keyParts_;
    std::shared_ptr<const FieldLocation> location_;
};

//----------------------------------------------------------------------------------------------------------------------

class FDBListImplBase {

public:

    FDBListImplBase();
    virtual ~FDBListImplBase();

    virtual bool next(FDBListElement& elem) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

class FDBListObject {

public: // methods

    FDBListObject(FDBListImplBase* impl);

    // Get the next element. Return false if at end.
    bool next(FDBListElement& elem);

private:

    std::unique_ptr<FDBListImplBase> impl_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
