/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   IndexAxis.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_IndexAxis_H
#define fdb5_IndexAxis_H

#include <iosfwd>
#include <map>
#include <memory>

#include "eckit/container/DenseSet.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/types/Types.h"

namespace eckit {
class Stream;
}

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class IndexAxis : private eckit::NonCopyable {

public: // methods

    IndexAxis();
    IndexAxis(eckit::Stream &s, const int version);

    ~IndexAxis();

    void insert(const Key &key);
    void encode(eckit::Stream &s, const int version) const;

    // Decode can be used for two-stage initialisation (IndexAxis a; a.decode(s);)
    void decode(eckit::Stream& s, const int version);

    const eckit::DenseSet<std::string> &values(const std::string &keyword) const;

    void dump(std::ostream &out, const char* indent) const;

    bool contains(const Key& key) const;

    /// Provide a means to test if the index has changed since it was last written out, and to
    /// mark that it has been written out.
    bool dirty() const;
    void clean();

    /// Sort the internal axes
    void sort();

    /// Reset the axis to a default state.
    void wipe();

    friend std::ostream &operator<<(std::ostream &s, const IndexAxis &x) {
        x.print(s);
        return s;
    }

private: // methods

    void encode(eckit::Stream &s) const;
    void encodeLegacy(eckit::Stream &s) const;
    
    void decode(eckit::Stream& s);
    void decodeLegacy(eckit::Stream& s);

    void print(std::ostream &out) const;


private: // members

    typedef std::map<std::string, std::shared_ptr<eckit::DenseSet<std::string> > > AxisMap;
    AxisMap axis_;

    bool readOnly_;
    bool dirty_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
