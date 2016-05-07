/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   PurgeVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_PurgeVisitor_H
#define fdb5_PurgeVisitor_H

#include <iosfwd>
#include <set>
#include <map>
#include <vector>

#include "eckit/filesystem/PathName.h"

#include "fdb5/database/Index.h"
#include "fdb5/database/Field.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct Stats {

    Stats() : totalFields_(0), duplicates_(0), wiped_(0),
            totalSize_(0), duplicatesSize_(0), wipedSize_(0) {}

    size_t totalFields_;
    size_t duplicates_;
    size_t wiped_;

    eckit::Length totalSize_;
    eckit::Length duplicatesSize_;
    eckit::Length wipedSize_;

    Stats &operator+=(const Stats &rhs) {
        totalFields_ += rhs.totalFields_;
        duplicates_ += rhs.duplicates_;
        wiped_ += rhs.wiped_;
        totalSize_ += rhs.totalSize_;
        duplicatesSize_ += rhs.duplicatesSize_;
        wipedSize_ += rhs.wipedSize_;

        return *this;
    }

    friend std::ostream &operator<<(std::ostream &s, const Stats &x) {
        x.print(s);
        return s;
    }

    void print(std::ostream &out) const;

};
//----------------------------------------------------------------------------------------------------------------------

class PurgeVisitor : public EntryVisitor, public TocWiper {
public:

    PurgeVisitor(const eckit::PathName &directory);
    ~PurgeVisitor();

    Stats totals() const;

    // void currentIndex(const Index* index);

    void report(std::ostream &out) const;

    void purge() const;

private: // methods



    virtual void visit(const Index& index,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint,
                       const Field& field);

    virtual void wipe(const std::vector<Index*>&);


private: // members

    eckit::PathName directory_;

    std::set<eckit::PathName> activeDataFiles_;
    std::set<eckit::PathName> allDataFiles_;
    std::set<eckit::PathName> wiped_;
    std::map<eckit::PathName, size_t> indexUsage_;
    std::map<eckit::PathName, size_t> dataUsage_;

    std::set<std::string> active_;

    std::map<const Index*, Stats> indexStats_;
    std::set<Index*> delete_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
