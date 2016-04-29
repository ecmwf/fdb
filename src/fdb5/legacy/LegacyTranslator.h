/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino
/// @date June 2013

#ifndef fdb5_LegacyTranslator_H
#define fdb5_LegacyTranslator_H

#include <string>
#include <map>

#include "eckit/types/Types.h"
#include "eckit/memory/NonCopyable.h"

namespace fdb5 {

class Key;

namespace legacy {

//-----------------------------------------------------------------------------

class LegacyTranslator : private eckit::NonCopyable {

public: // methods

    LegacyTranslator();

    void set(Key &key, const std::string &keyword, const std::string &value) const;

private: // members

    typedef eckit::StringDict::value_type (*translator_t) (const Key &key, const std::string &, const std::string &);
    typedef std::map< std::string, translator_t > store_t;

    store_t translators_;
};

//-----------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5

#endif
