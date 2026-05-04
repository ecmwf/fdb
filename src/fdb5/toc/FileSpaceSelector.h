/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


/// @author Metin Cakircali
/// @date   June 2026

#pragma once

#include <iosfwd>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/utils/Regex.h"
#include "metkit/mars/Matcher.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class FileSpaceSelector {

public:  // methods

    virtual ~FileSpaceSelector() = default;
    virtual bool match(const Key& key) const = 0;

private:  // methods

    virtual void print(std::ostream& out) const = 0;
    friend std::ostream& operator<<(std::ostream& out, const FileSpaceSelector& selector) {
        selector.print(out);
        return out;
    }
};

//----------------------------------------------------------------------------------------------------------------------

/// A FileSpaceSelector that uses a regex to match against the serialised key.
class RegexSelector : public FileSpaceSelector {

public:  // methods

    RegexSelector(eckit::Regex regex);

    bool match(const Key& key) const override;

private:  // methods

    void print(std::ostream& out) const override;

private:  // members

    eckit::Regex regex_;
};

//----------------------------------------------------------------------------------------------------------------------
// A FileSpaceSelector that uses a metkit::mars::Matcher to match against the key. This allows for more structured
// matching based on the individual key components, rather than the serialised key as a whole (as with RegexSelector).
class MatchSelector : public FileSpaceSelector {
public:  // factory

    static metkit::mars::Matcher buildMatcher(const eckit::LocalConfiguration& cfg);

public:  // methods

    MatchSelector(metkit::mars::Matcher matcher);

    bool match(const Key& key) const override;

private:  // methods

    void print(std::ostream& out) const override;

private:  // members

    metkit::mars::Matcher matcher_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
