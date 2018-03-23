/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Retriever.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_Retriever_H
#define fdb5_Retriever_H

#include <iosfwd>
#include <cstdlib>
#include <map>

#include "eckit/memory/NonCopyable.h"
#include "eckit/container/CacheLRU.h"
#include "eckit/config/LocalConfiguration.h"

namespace eckit {
class DataHandle;
}

class MarsTask;

namespace fdb5 {

class Key;
class Op;
class DB;
class Schema;
class Notifier;

//----------------------------------------------------------------------------------------------------------------------

class Retriever : public eckit::NonCopyable {

public: // methods

    Retriever(const eckit::Configuration& dbConfig=eckit::LocalConfiguration());

    ~Retriever();

    /// Retrieves the data selected by the MarsRequest to the provided DataHandle
    /// @returns  data handle to read from

    eckit::DataHandle *retrieve(const MarsTask &task) const;

    /// Retrieves the data selected by the MarsRequest to the provided DataHandle
    /// @param notifyee is an object that handles notifications for the client, e.g. wind conversion
    /// @returns  data handle to read from

    eckit::DataHandle* retrieve(const MarsTask& task, const Notifier& notifyee) const;

    friend std::ostream &operator<<(std::ostream &s, const Retriever &x) {
        x.print(s);
        return s;
    }

private: // methods

    void print(std::ostream &out) const;

    eckit::DataHandle* retrieve(const MarsTask &task, const Schema &schema, bool sorted, const Notifier& notifyee) const;

private: // data

    mutable eckit::CacheLRU<Key,DB*> databases_;

    eckit::LocalConfiguration dbConfig_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
