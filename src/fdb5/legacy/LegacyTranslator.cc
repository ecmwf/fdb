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

#include "marslib/Param.h"

#include "fdb5/Key.h"
#include "fdb5/legacy/LegacyTranslator.h"

using namespace eckit;

namespace fdb5 {
namespace legacy {

//-----------------------------------------------------------------------------

static bool iswave(const Key& key) {

    static const char* wavestreams[] = { "wave", "waef", "weov", "dcwv", "scwv", "ewda", 0 };

    const std::string& stream = key.get("stream");
    int i = 0;
    while(wavestreams[i]) {
        if(stream == wavestreams[i]) {
            return true;
        }
        i++;
    }
    return false;
}

static StringDict::value_type integer(const Key& key, const std::string& keyword, const std::string& value )
{
	static Translator<std::string,long> s2l;
	static Translator<long,std::string> l2s;

	return StringDict::value_type(keyword, l2s(s2l(value)));

}

static StringDict::value_type levtype(const Key& key, const std::string& keyword, const std::string& value )
{
    static const char * levtype_ = "levtype";

    if ( value == "s" )
        return StringDict::value_type( levtype_ , "sfc" );

    if ( value == "m" )
        return StringDict::value_type( levtype_ , "ml" );

    if ( value == "p" )
        return StringDict::value_type( levtype_ , "pl" );

    if ( value == "t" )
        return StringDict::value_type( levtype_ , "pt" );

    if ( value == "v" )
        return StringDict::value_type( levtype_ , "pv" );

    return StringDict::value_type( levtype_ , value );
}

static StringDict::value_type repres(const Key& key, const std::string& keyword, const std::string& value )
{
    return StringDict::value_type( "repres" , value );
}

static StringDict::value_type levelist(const Key& key, const std::string& keyword, const std::string& value )
{
    static const char * levelist_ = "levelist";

    if( StringTools::startsWith(value,"0") )
        return StringDict::value_type( levelist_, StringTools::front_trim(value,"0") );

    return StringDict::value_type( levelist_ , value );
}

static StringDict::value_type param(const Key& key, const std::string& keyword, const std::string& value )
{
    Param p(value);

    if(p.table() == 0) {
        if(p.value() < 1000) {
            if(iswave(key)) {
                p = Param(140, p.value());
            }
            else {
                p = Param(128, p.value());
            }
        }
    }

    return StringDict::value_type( "param" , std::string(p) );
}

static StringDict::value_type channel(const Key& key, const std::string& keyword, const std::string& value )
{
    return StringDict::value_type( "channel" , value );
}

static StringDict::value_type time(const Key& key, const std::string& keyword, const std::string& value )
{
    static const char * time_ = "time";

    if( value.size() == 2 )
        return StringDict::value_type( time_ , value + "00" );

    return StringDict::value_type( time_ , value );
}

static StringDict::value_type step(const Key& key, const std::string& keyword, const std::string& value )
{
    static const char * step_ = "step";

    unsigned int v = Translator<std::string,unsigned int>()(value);

    return StringDict::value_type( step_ , Translator<unsigned int,std::string>()(v) );
}

//-----------------------------------------------------------------------------

LegacyTranslator::LegacyTranslator()
{
    translators_["levt"]     = &levtype;
    translators_["levtype"]  = &levtype;
    translators_["levty"]    = &levtype;

    translators_["iteration"]  = &integer;

    translators_["repr"]  = &repres;

    translators_["step"]  = &step;

    translators_["LEVEL"] = &levelist;
    translators_["level"] = &levelist;

    translators_["parameter"] = &param;
    translators_["PARAMETER"] = &param;

    translators_["CHANNEL"] = &channel;

    translators_["time"]   = &time;
    translators_["TIME"]   = &time;
}

void legacy::LegacyTranslator::set(Key& key, const std::string& keyword, const std::string& value) const
{
        store_t::const_iterator i = translators_.find(keyword);
        if( i != translators_.end() ) {
            StringDict::value_type kv = (*(*i).second)(key, keyword, value);
            key.set(kv.first, kv.second);
        }
        else {
            key.set(keyword, value);
        }
}

//-----------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
