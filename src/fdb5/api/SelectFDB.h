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
/// @date   Mar 2018

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "metkit/mars/Matcher.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/rules/SelectMatcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
class SelectFDB : public FDBBase {

private:  // types


    class FDBLane {
        SelectMatcher matcher_;
        Config config_;
        std::optional<FDB> fdb_;

    public:

        FDBLane(const eckit::LocalConfiguration& config);

        FDB& get();

        void flush();

        template <typename T>  // T is either a mars request or a Key
        bool matches(const T& vals, metkit::mars::Matcher::MatchMissingPolicy matchOnMissing) const;
    };


public:  // methods

    SelectFDB(const Config& config, const std::string& name);

    ~SelectFDB() override;

    void archive(const Key& key, const void* data, size_t length) override;

    ListIterator inspect(const metkit::mars::MarsRequest& request) override;

    ListIterator list(const FDBToolRequest& request, int level) override;

    DumpIterator dump(const FDBToolRequest& request, bool simple) override;

    StatusIterator status(const FDBToolRequest& request) override;

    WipeIterator wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) override;

    PurgeIterator purge(const FDBToolRequest& request, bool doit, bool porcelain) override;

    StatsIterator stats(const FDBToolRequest& request) override;

    ControlIterator control(const FDBToolRequest& request, ControlAction action,
                            ControlIdentifiers identifiers) override;

    MoveIterator move(const FDBToolRequest& request, const eckit::URI& dest) override { NOTIMP; }

    AxesIterator axesIterator(const FDBToolRequest& request, int level) override;

    void flush() override;

private:  // methods

    void print(std::ostream& s) const override;

    template <typename QueryFN>
    auto queryInternal(const FDBToolRequest& request, const QueryFN& fn) -> decltype(fn(*(FDB*)(nullptr), request));

private:  // members

    std::vector<FDBLane> subFdbs_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
