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

#ifndef fdb_LegacyTranslator_H
#define fdb_LegacyTranslator_H

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"

namespace fdb5 {

//-----------------------------------------------------------------------------

class LegacyTranslator {

public: // methods

    LegacyTranslator();

    eckit::StringDict::value_type translate( const std::string& key, const std::string& value );

private: // members

    typedef eckit::StringDict::value_type (*translator_t) ( const std::string&, const std::string& );
    typedef std::map< std::string, translator_t > store_t;

    store_t translators_;

};

//-----------------------------------------------------------------------------

} // namespace fdb5

#endif
