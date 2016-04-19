/*
 * (C) Copyright 1996-2013 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/parser/StringTools.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LegacyTranslator.h"

using namespace eckit;

namespace fdb5 {

//-----------------------------------------------------------------------------

StringDict::value_type levtype( const std::string& key, const std::string& value )
{
    static const char * levtype_ = "levtype";

    if ( value == "s" )
        return StringDict::value_type( levtype_ , "sfc" );

    if ( value == "m" )
        return StringDict::value_type( levtype_ , "ml" );

    if ( value == "p" )
        return StringDict::value_type( levtype_ , "pl" );

    return StringDict::value_type( levtype_ , value );
}

StringDict::value_type repres( const std::string& key, const std::string& value )
{
    return StringDict::value_type( "repres" , value );
}

StringDict::value_type levelist( const std::string& key, const std::string& value )
{
    static const char * levelist_ = "levelist";

    if( StringTools::startsWith(value,"0") )
        return StringDict::value_type( levelist_, StringTools::front_trim(value,"0") );

    return StringDict::value_type( levelist_ , value );
}

StringDict::value_type param( const std::string& key, const std::string& value )
{
    if( StringTools::startsWith(value,"0") )
        return StringDict::value_type( "param", StringTools::front_trim(value,"0") );

    return StringDict::value_type( "param" , value );
}

StringDict::value_type channel( const std::string& key, const std::string& value )
{
    return StringDict::value_type( "channel" , value );
}

StringDict::value_type time( const std::string& key, const std::string& value )
{
    static const char * time_ = "time";

    if( value.size() == 2 )
        return StringDict::value_type( time_ , value + "00" );

    return StringDict::value_type( time_ , value );
}

StringDict::value_type step( const std::string& key, const std::string& value )
{
    static const char * step_ = "step";

    unsigned int v = Translator<std::string,unsigned int>()(value);

    return StringDict::value_type( step_ , Translator<unsigned int,std::string>()(v) );
}

//-----------------------------------------------------------------------------

LegacyTranslator::LegacyTranslator()
{
    translators_["levt"]  = &fdb5::levtype;
    translators_["levty"] = &fdb5::levtype;

    translators_["repr"]  = &fdb5::repres;

    translators_["step"]  = &fdb5::step;

    translators_["LEVEL"] = &fdb5::levelist;
    translators_["level"] = &fdb5::levelist;

    translators_["parameter"] = &fdb5::param;
    translators_["PARAMETER"] = &fdb5::param;

    translators_["CHANNEL"] = &fdb5::channel;

    translators_["time"]   = &fdb5::time;
    translators_["TIME"]   = &fdb5::time;
}

StringDict::value_type LegacyTranslator::translate(const std::string& key, const std::string& value)
{
    store_t::const_iterator i = translators_.find(key);
    if( i != translators_.end() )
        return (*(*i).second)( key, value );
    else
        return StringDict::value_type( key, value );
}

//-----------------------------------------------------------------------------

} // namespace fdb5
