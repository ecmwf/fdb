/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cctype>

#include "eckit/parser/StringTools.h"
#include "eckit/utils/Translator.h"

#include "marslib/Param.h"

#include "fdb5/Key.h"
#include "fdb5/legacy/LegacyTranslator.h"

using namespace eckit;

namespace fdb5 {
namespace legacy {

//-----------------------------------------------------------------------------

#if 0 // unused
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
#endif

static StringDict::value_type integer(const Key& key, const std::string& keyword, const std::string& value )
{
    static Translator<std::string,long> s2l;
    static Translator<long,std::string> l2s;

    return StringDict::value_type(keyword, l2s(s2l(value)));

}

static StringDict::value_type real(const Key& key, const std::string& keyword, const std::string& value )
{
    static Translator<std::string,double> s2r;
    static Translator<double,std::string> r2s;

    return StringDict::value_type(keyword, r2s(s2r(value)));

}

static StringDict::value_type levelist(const Key& key, const std::string& keyword, const std::string& value )
{
    static Translator<std::string,double> s2r;
    static Translator<double,std::string> r2s;

    return StringDict::value_type("levelist", r2s(s2r(value)));

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

static StringDict::value_type param(const Key& key, const std::string& keyword, const std::string& value )
{
    Param p(value);

    // if(p.table() == 0) {
    //     if(p.value() < 1000) {
    //         if(iswave(key)) {
    //             p = Param(140, p.value());
    //         }
    //         else {
    //             p = Param(128, p.value());
    //         }
    //     }
    // }

    return StringDict::value_type( "param" , std::string(p) );
}

static StringDict::value_type time(const Key& key, const std::string& keyword, const std::string& value )
{
    static const char * time_ = "time";

    if( value.size() == 2 )
        return StringDict::value_type( time_ , value + "00" );

    return StringDict::value_type( time_ , value );
}

//-----------------------------------------------------------------------------

LegacyTranslator::LegacyTranslator()
{
    translators_["levt"]     = &levtype;
    translators_["levtype"]  = &levtype;
    translators_["levty"]    = &levtype;

    translators_["iteration"]  = &integer;

    translators_["repr"]  = &repres;

    translators_["step"]  = &real;

    translators_["level"]    = &levelist;
    translators_["levelist"] = &levelist;

    translators_["parameter"] = &param;

    translators_["channel"] = &integer;

    translators_["time"]   = &time;
}

void legacy::LegacyTranslator::set(Key& key, const std::string& keyword, const std::string& value) const
{
        std::string k(keyword);
        std::string v(value);

        std::transform(k.begin(), k.end(), k.begin(), tolower);
        std::transform(v.begin(), v.end(), v.begin(), tolower);

        if(v == "off") {
            key.unset(k);
            return;
        }

        store_t::const_iterator i = translators_.find(k);
        if( i != translators_.end() ) {
            StringDict::value_type kv = (*(*i).second)(key, k, v);
            key.set(kv.first, kv.second);
        }
        else {
            key.set(k, v);
        }
}

//-----------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
