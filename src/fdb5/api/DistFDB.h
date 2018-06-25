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
    ~DistFDB();

    virtual void archive(const Key& key, const void* data, size_t length);

    virtual eckit::DataHandle* retrieve(const MarsRequest& request);

    virtual std::string id() const;

    virtual void flush();

    virtual FDBStats stats() const;

private: // methods

    virtual void print(std::ostream& s) const;

private:

    eckit::RendezvousHash hash_;

    std::vector<FDB> lanes_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_api_DistFDB_H
