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
// #include "eckit/filesystem/PathName.h"
#include "eckit/types/Types.h"

namespace eckit {
class Stream;
}

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Key;

// class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class IndexAxis : private eckit::NonCopyable {

public:  // methods

    IndexAxis();
    IndexAxis(eckit::Stream& s, const int version);
    IndexAxis(IndexAxis&& rhs) noexcept;

    IndexAxis& operator=(IndexAxis&& rhs) noexcept;

    ~IndexAxis();

    bool operator==(const IndexAxis& rhs) const;
    bool operator!=(const IndexAxis& rhs) const;

    void insert(const Key& key);
    /// @note: the values are required to be cannonicalised
    void insert(const std::string& axis, const std::vector<std::string>& values);
    size_t encodeSize(const int version) const;
    void encode(eckit::Stream& s, const int version) const;
    static int currentVersion() { return 3; }

    void merge(const IndexAxis& other);

    // Decode can be used for two-stage initialisation (IndexAxis a; a.decode(s);)
    void decode(eckit::Stream& s, const int version);

    bool has(const std::string& keyword) const;
    const eckit::DenseSet<std::string>& values(const std::string& keyword) const;

    std::map<std::string, eckit::DenseSet<std::string>> map() const;

    void dump(std::ostream& out, const char* indent) const;

    bool partialMatch(const metkit::mars::MarsRequest& request) const;
    bool contains(const Key& key) const;
    bool containsPartial(const Key& key) const;

    /// Provide a means to test if the index has changed since it was last written out, and to
    /// mark that it has been written out.
    bool dirty() const;
    void clean();

    /// Sort the internal axes
    void sort();

    /// Reset the axis to a default state.
    void wipe();

    friend std::ostream& operator<<(std::ostream& s, const IndexAxis& x) {
        x.print(s);
        return s;
    }

    friend eckit::JSON& operator<<(eckit::JSON& j, const IndexAxis& x) {
        x.json(j);
        return j;
    }

private:  // methods

    void encodeCurrent(eckit::Stream& s, const int version) const;
    void encodeLegacy(eckit::Stream& s, const int version) const;

    void decodeCurrent(eckit::Stream& s, const int version);
    void decodeLegacy(eckit::Stream& s, const int version);

    void print(std::ostream& out) const;
    void json(eckit::JSON& j) const;

private:  // members

    typedef std::map<std::string, std::shared_ptr<eckit::DenseSet<std::string>>> AxisMap;
    AxisMap axis_;

    bool readOnly_;
    bool dirty_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
