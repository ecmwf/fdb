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
/// @date   Mar 2018

#ifndef fdb5_api_DistFDB_H
#define fdb5_api_DistFDB_H

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"

#include "eckit/utils/RendezvousHash.h"


namespace fdb5 {

class FDB;

//----------------------------------------------------------------------------------------------------------------------


class DistFDB : public FDBBase {

public: // method

    DistFDB(const eckit::Configuration& config, const std::string& name);
    ~DistFDB() override;

    virtual void archive(const Key& key, const void* data, size_t length) override;

    virtual eckit::DataHandle* retrieve(const MarsRequest& request) override;

    virtual ListIterator list(const FDBToolRequest& request) override;

    virtual DumpIterator dump(const FDBToolRequest& request, bool simple) override;

    virtual WhereIterator where(const FDBToolRequest& request) override;

    virtual WipeIterator wipe(const FDBToolRequest& request, bool doit, bool verbose) override;

    virtual PurgeIterator purge(const FDBToolRequest& request, bool doit) override;

    virtual StatsIterator stats(const FDBToolRequest& request) override;

    virtual void flush() override;

    virtual FDBStats stats() const override;

private: // methods

    virtual void print(std::ostream& s) const override;

    template <typename QueryFN>
    auto queryInternal(const FDBToolRequest& request, const QueryFN& fn) -> decltype(fn(*(FDB*)(nullptr), request));

private:

    eckit::RendezvousHash hash_;

    std::vector<FDB> lanes_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_api_DistFDB_H
