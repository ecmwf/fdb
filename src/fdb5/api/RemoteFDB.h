/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @author Emanuele Danovaro
/// @author Chris Bradley
/// @date   Mar 2018

#pragma once

#include <thread>
#include <unordered_map>

#include "fdb5/api/LocalFDB.h"
#include "fdb5/remote/client/Client.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
class Archiver;
class RemoteFDBClient;

class RemoteFDB : public LocalFDB {

public:  // method

    RemoteFDB(const eckit::Configuration& config, const std::string& name);
    ~RemoteFDB() override {}

    ListIterator inspect(const metkit::mars::MarsRequest& request) override;

    ListIterator list(const FDBToolRequest& request, int depth) override;

    AxesIterator axesIterator(const FDBToolRequest& request, int depth = 3) override;

    DumpIterator dump(const FDBToolRequest& request, bool simple) override { NOTIMP; }

    StatusIterator status(const FDBToolRequest& request) override { NOTIMP; }

    WipeIterator wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) override { NOTIMP; }

    PurgeIterator purge(const FDBToolRequest& request, bool doit, bool porcelain) override { NOTIMP; }

    StatsIterator stats(const FDBToolRequest& request) override;

    ControlIterator control(const FDBToolRequest& request, ControlAction action,
                            ControlIdentifiers identifiers) override {
        NOTIMP;
    }

    MoveIterator move(const FDBToolRequest& request, const eckit::URI& dest) override { NOTIMP; }

private:  // methods

    void print(std::ostream& s) const override;


private:  // members

    /// @brief The RemoteFDBClient instance is used to communicate with remote service.
    /// It is held in a shared_ptr so that it can be kept alive by any Iterator instances (e.g. list, inspect)
    /// created by this FDB object.
    std::shared_ptr<RemoteFDBClient> client_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
