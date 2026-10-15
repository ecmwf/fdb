// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   Inspector.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/config/Config.h"

#include "eckit/container/CacheLRU.h"

#include <cstdlib>
#include <iosfwd>
#include <mutex>
#include <vector>

namespace eckit {
class DataHandle;
}

namespace metkit::mars {
class MarsRequest;
}


namespace fdb5 {

class Key;
class Op;
class CatalogueReader;
class Schema;
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

class Inspector {

public:  // methods

    explicit Inspector(const Config& dbConfig);

    Inspector(const Inspector&) = delete;
    Inspector& operator=(const Inspector&) = delete;
    Inspector(Inspector&&) = delete;
    Inspector& operator=(Inspector&&) = delete;

    /// Retrieves the data selected by the MarsRequest to the provided DataHandle
    /// @returns  data handle to read from

    ListIterator inspect(const metkit::mars::MarsRequest& request) const;

    /// Give read access to a range of entries according to a request

    void visitEntries(const FDBToolRequest& request, EntryVisitor& visitor) const;

    friend std::ostream& operator<<(std::ostream& s, const Inspector& x) {
        x.print(s);
        return s;
    }

private:  // methods

    void print(std::ostream& out) const;

private:  // data

    mutable std::mutex mutex_;

    mutable eckit::CacheLRU<Key, CatalogueReader*> databases_;

    Config dbConfig_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
