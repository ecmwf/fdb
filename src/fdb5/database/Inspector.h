/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Inspector.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_Inspector_H
#define fdb5_Inspector_H

#include <cstdlib>
#include <iosfwd>
#include <map>

#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/config/Config.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/container/CacheLRU.h"
#include "eckit/memory/NonCopyable.h"

namespace eckit {
class DataHandle;
}

namespace metkit {
namespace mars {
class MarsRequest;
}
}  // namespace metkit

namespace fdb5 {

class Key;
class Op;
class CatalogueReader;
class Schema;
class Notifier;
class FDBToolRequest;
class EntryVisitor;

//----------------------------------------------------------------------------------------------------------------------

class InspectIterator : public APIIteratorBase<ListElement> {
public:

    InspectIterator();
    ~InspectIterator();

    void emplace(ListElement&& elem);
    bool next(ListElement& elem) override;

private:

    std::vector<ListElement> queue_;
    size_t index_;
};

//----------------------------------------------------------------------------------------------------------------------

class Inspector : public eckit::NonCopyable {

public:  // methods

    Inspector(const Config& dbConfig);

    ~Inspector();

    /// Retrieves the data selected by the MarsRequest to the provided DataHandle
    /// @returns  data handle to read from

    ListIterator inspect(const metkit::mars::MarsRequest& request) const;

    /// Retrieves the data selected by the MarsRequest to the provided DataHandle
    /// @param notifyee is an object that handles notifications for the client, e.g. wind conversion
    /// @returns  data handle to read from

    ListIterator inspect(const metkit::mars::MarsRequest& request, const Notifier& notifyee) const;

    /// Give read access to a range of entries according to a request

    void visitEntries(const FDBToolRequest& request, EntryVisitor& visitor) const;

    friend std::ostream& operator<<(std::ostream& s, const Inspector& x) {
        x.print(s);
        return s;
    }

private:  // methods

    void print(std::ostream& out) const;

    ListIterator inspect(const metkit::mars::MarsRequest& request, const Schema& schema,
                         const Notifier& notifyee) const;

private:  // data

    mutable eckit::CacheLRU<Key, CatalogueReader*> databases_;

    Config dbConfig_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
