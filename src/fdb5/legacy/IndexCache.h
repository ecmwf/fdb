/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#ifndef IndexCache_H
#define IndexCache_H

#include "eckit/thread/Mutex.h"
#include "eckit/filesystem/PathName.h"

#include "pointdb/GribFileSummary.h"

namespace eckit { class JSON; }

namespace fdb5 {
namespace legacy {

class FieldInfoKey;

//----------------------------------------------------------------------------------------------------------------------

class IndexCache {
public:
    IndexCache( const eckit::PathName& );
    virtual ~IndexCache();

    void ready(const eckit::PathName&, const GribFileSummary&);
    std::vector<eckit::PathName> candidates(const FieldInfoKey&, const FieldInfoKey&);

    eckit::PathName btree(const eckit::PathName& path);
    eckit::PathName summary(const eckit::PathName& path);

    bool ready(const eckit::PathName&);
    void json(eckit::JSON&) const;


protected:

    void process(const eckit::PathName&);

private:

    void update(const eckit::PathName&, const GribFileSummary&);

    std::map<eckit::PathName, GribFileSummary> ready_;
    std::set<eckit::PathName> done_;

    eckit::Mutex mutex_;
    eckit::PathName cache_;

    unsigned long long files_;
    unsigned long long fields_;
    unsigned long long points_;
    unsigned long long bytes_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5

#endif
