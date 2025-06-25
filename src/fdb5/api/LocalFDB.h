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

#ifndef fdb5_api_LocalFDB_H
#define fdb5_api_LocalFDB_H

#include "fdb5/api/FDBFactory.h"
#include "fdb5/database/Reindexer.h"


namespace fdb5 {

class Inspector;
class Archiver;
class FDB;

//----------------------------------------------------------------------------------------------------------------------

class LocalFDB : public FDBBase {

public:  // methods

    using FDBBase::FDBBase;

    void archive(const Key& key, const void* data, size_t length) override;

    void reindex(const Key& key, const FieldLocation& location) override;

    ListIterator inspect(const metkit::mars::MarsRequest& request) override;

    ListIterator list(const FDBToolRequest& request, int level) override;

    DumpIterator dump(const FDBToolRequest& request, bool simple) override;

    StatusIterator status(const FDBToolRequest& request) override;

    WipeIterator wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) override;

    PurgeIterator purge(const FDBToolRequest& request, bool doit, bool porcelain) override;

    StatsIterator stats(const FDBToolRequest& request) override;

    ControlIterator control(const FDBToolRequest& request, ControlAction action,
                            ControlIdentifiers identifiers) override;

    MoveIterator move(const FDBToolRequest& request, const eckit::URI& dest) override;

    AxesIterator axesIterator(const FDBToolRequest& request, int axes) override;

    void flush() override;

protected:  // methods

    template <typename VisitorType, typename... Ts>
    APIIterator<typename VisitorType::ValueType> queryInternal(const FDBToolRequest& request, Ts... args);

private:  // methods

    void print(std::ostream& s) const override;

protected:  // members

    std::string home_;

    std::unique_ptr<Archiver> archiver_;
    std::unique_ptr<Reindexer> reindexer_;
    std::unique_ptr<Inspector> inspector_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_api_LocalFDB_H
