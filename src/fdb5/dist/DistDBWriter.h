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

#ifndef fdb5_dist_DistDBWriter_H
#define fdb5_dist_DistDBWriter_H

#include "fdb5/dist/DistDB.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

class Archiver;

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class DistDBWriter : public DistDB {

public: // methods

    DistDBWriter(const Key &key, const eckit::Configuration& config);
    DistDBWriter(const eckit::PathName& directory, const eckit::Configuration& config);

    virtual ~DistDBWriter();

private: // methods

    virtual void print(std::ostream& out) const;

    virtual bool selectIndex(const Key &key);
    virtual void deselectIndex();

    void checkSchema(const Key &key) const;
    virtual void archive(const Key &key, const void *data, eckit::Length length);
    virtual void flush();

private: // members

    std::vector<Archiver*> archivers_;

    Key indexKey_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_dist_DistDBWriter_H
