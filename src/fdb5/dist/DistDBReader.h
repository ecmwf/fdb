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

#ifndef fdb5_dist_DistDBReader_H
#define fdb5_dist_DistDBReader_H

#include "fdb5/dist/DistDB.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DistDBReader : public DistDB {

public: // methods

    DistDBReader(const Key& key, const eckit::Configuration& config);
    DistDBReader(const eckit::PathName& directory, const eckit::Configuration& config);

    virtual ~DistDBReader();

private: // methods

    virtual void print(std::ostream& out) const;

    virtual bool selectIndex(const Key &key);
    virtual void deselectIndex();
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_dist_DistDBReader_H
