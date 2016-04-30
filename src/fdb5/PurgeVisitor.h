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

#include "fdb5/Index.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct Stats {

    Stats() : totalFields_(0), duplicates_(0), totalSize_(0), duplicatesSize_(0) {}

    size_t totalFields_;
    size_t duplicates_;
    eckit::Length totalSize_;
    eckit::Length duplicatesSize_;

    Stats &operator+=(const Stats &rhs) {
        totalFields_ += rhs.totalFields_;
        duplicates_ += rhs.duplicates_;
        totalSize_ += rhs.totalSize_;
        duplicatesSize_ += rhs.duplicatesSize_;
        return *this;
    }

    friend std::ostream &operator<<(std::ostream &s, const Stats &x) {
        x.print(s);
        return s;
    }

    void print(std::ostream &out) const;

};
//----------------------------------------------------------------------------------------------------------------------

class PurgeVisitor : public EntryVisitor {
public:

    PurgeVisitor(const eckit::PathName &directory);

    Stats totals() const;

    // void currentIndex(const Index* index);

    void report(std::ostream &out) const;

    void purge() const;

private: // methods



    virtual void visit(const Index& index,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint,
                       const eckit::PathName &path,
                       eckit::Offset offset,
                       eckit::Length length);

private: // members

    eckit::PathName directory_;

    std::set<eckit::PathName> activeDataFiles_;
    std::set<eckit::PathName> allDataFiles_;
    std::map<eckit::PathName, size_t> indexUsage_;
    std::map<eckit::PathName, size_t> dataUsage_;

    std::set<std::string> active_;

    std::map<const Index*, Stats> indexStats_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
