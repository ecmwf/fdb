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
#include "fdb5/database/Inspector.h"
#include "fdb5/database/Reindexer.h"


namespace fdb5 {

class Archiver;
class FDB;

//----------------------------------------------------------------------------------------------------------------------

class LocalFDB : public FDBBase {

public:  // methods

    using FDBBase::FDBBase;

    void archive(const Key& key, const void* data, size_t length, const std::string& tracingID) override;

    void reindex(const Key& key, const FieldLocation& location, const std::string& tracingID) override;

    ListIterator inspect(const metkit::mars::MarsRequest& request, const std::string& tracingID) override;

    ListIterator list(const FDBToolRequest& request, const std::string& tracingID, int level) override;

    DumpIterator dump(const FDBToolRequest& request, const std::string& tracingID, bool simple) override;

    StatusIterator status(const FDBToolRequest& request, const std::string& tracingID) override;

    WipeStateIterator wipe(const FDBToolRequest& request, const std::string& tracingID, bool doit, bool porcelain,
                           bool unsafeWipeAll) override;

    PurgeIterator purge(const FDBToolRequest& request, const std::string& tracingID, bool doit,
                        bool porcelain) override;

    StatsIterator stats(const FDBToolRequest& request, const std::string& tracingID) override;

    ControlIterator control(const FDBToolRequest& request, const std::string& tracingID, ControlAction action,
                            ControlIdentifiers identifiers) override;

    MoveIterator move(const FDBToolRequest& request, const std::string& tracingID, const eckit::URI& dest) override;

    AxesIterator axesIterator(const FDBToolRequest& request, const std::string& tracingID, int axes) override;

    void flush(const std::string& tracingID) override;

protected:  // methods

    template <typename VisitorType, typename... Ts>
    APIIterator<typename VisitorType::ValueType> queryInternal(const FDBToolRequest& request,
                                                               const std::string& tracingID, Ts... args);

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
