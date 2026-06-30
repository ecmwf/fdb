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
/// @date Nov 2016

#ifndef fdb5_TocIndexLocation_H
#define fdb5_TocIndexLocation_H

#include <sys/types.h>

#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"

#include "fdb5/database/IndexLocation.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class TocIndexLocation : public IndexLocation {

public:  // methods

    TocIndexLocation(const eckit::PathName& path, off_t offset);
    TocIndexLocation(eckit::Stream&);

    off_t offset() const;

    //    eckit::PathName path() const override;
    eckit::URI uri() const override;

    IndexLocation* clone() const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    void encode(eckit::Stream&) const override;

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<TocIndexLocation> reanimator_;

private:  // methods

    void print(std::ostream& out) const override;

private:  // members

    eckit::PathName path_;

    off_t offset_;

private:  // friends

    friend class TocIndex;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_TocIndexLocation_H
