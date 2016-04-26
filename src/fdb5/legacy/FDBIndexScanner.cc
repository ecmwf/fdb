/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <ctype.h>

#include "eckit/io/StdFile.h"
#include "eckit/io/StdPipe.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Translator.h"
#include "eckit/types/Types.h"

#include "fdb5/legacy/FDBIndexScanner.h"
#include "fdb5/legacy/IndexCache.h"

using namespace eckit;

namespace fdb5 {
namespace legacy {

//-----------------------------------------------------------------------------

FDBIndexScanner::FDBIndexScanner(const PathName& path):
    path_(path)
{
    Log::info() << "FDBIndexScanner( path = " << path_ << ")" << std::endl;
}

FDBIndexScanner::~FDBIndexScanner()
{
}

void FDBIndexScanner::execute()
{
    Log::info() << "scanning " << path_ << std::endl;

    PathName lsfdb1( path_ + ".lsfdb" );
    PathName lsfdb2( path_ + "lsfdb" );

    if( lsfdb1.exists() )
    {
        Log::info() << "scanning file " << lsfdb1 << std::endl;

        StdFile f( lsfdb1, "r" );
        process(f);
        return;
    }

    if( lsfdb2.exists() )
    {
        Log::info() << "scanning file " << lsfdb2 << std::endl;

        StdFile f( lsfdb2, "r" );
        process(f);
        return;
    }

    // if all else fails, lets try to run the old tool 'lsfdb' (must be in path)
    {        
        std::string cmd(std::string("lsfdb ") + path_.asString());
        Log::info() << "scanning with pipe to command: [" << cmd << "]" << std::endl;
        StdPipe f(cmd, "r");
        process(f);
    }
}

void FDBIndexScanner::process(FILE* f)
{
    std::string ignore;
    std::string s;

    PathName dirpath = path_.dirName();

    // /ma_fdb/:od:oper:g:0001:20120617::/:fc:0000::::::::.

    std::vector<std::string> c;

     Log::info() << c << std::endl;
     Log::info() << path_ << std::endl;
//     Log::info() << path_.baseName() << std::endl;
//     Log::info() << std::string(path_.baseName()) << std::endl;

    std::string indexFileName(path_.baseName());
    Log::info() << "Index filename [" << indexFileName << "]" << std::endl;

    Tokenizer p(":");
    p(indexFileName, c);

     Log::info() << c.size() << std::endl;
     Log::info() << c << std::endl;

    ASSERT(c.size() >= 3);

    StringDict r;
    r["type"]   = c[0];
    r["time"]   = c[1];
    r["number"] = c[2];

    c.clear();

    // Log::info() << c << std::endl;
    // Log::info() << path_ << std::endl;
    // Log::info() << dirpath << std::endl;
    // Log::info() << dirpath.baseName() << std::endl;
    p(dirpath.baseName(),c);

    Log::info() << "[" << c << "]" << std::endl;

    ASSERT(c.size() >= 5);

    r["class"]    = c[0];
    r["type"]     = c[1];
    r["domain"]   = c[2];
    r["expver"]   = c[3];
    r["date"]     = c[4];

    try {

        Tokenizer parse("=");
        std::string prefix;

        char buf[10240];

        fgets(buf, sizeof(buf), f);

        while(fgets(buf, sizeof(buf), f))
        {
            StringDict m(r);

            std::string line(buf);

            Log::info() << line << std::endl;

            std::istringstream in(line);
            // e.g. 409802 FDB;  4558 0:6:0;  levelist=27 levtype=m parameter=155 step=6  3281188 (61798740)

            in >> ignore;
            in >> ignore;
            in >> ignore;
            in >> prefix;

            prefix = prefix.substr(0, prefix.length()-1); // chop off trailing ';' so prefix = 0:6:0

            Log::info() << "prefix = " << prefix << std::endl;

            std::string datapath = dirpath + "/:" + prefix + path_.baseName();

            Log::info() << "datapath = " << datapath << std::endl;

            size_t length;
            size_t offset;
            for(;;)
            {
                StringList v;
                in >> s;

                Log::info() << "s -> [" << s << "]" << std::endl;

                if(isdigit(s[0])) // when we hit digits, parse length and offset, e.g.   3281188 (61798740)
                {
                    length = Translator<std::string, size_t>()(s);
                    in >> s;
                    offset = Translator<std::string, size_t>()(s.substr(1,s.length()-2));
                    break;
                }

                // parse keywords, e.g. levelist=27 levtype=m parameter=155 step=6

                parse(s,v);
                m[v[0]] = v[1];
            }

            Log::info() << "Indexed field @ " << datapath << " offset=" << offset << " lenght=" << length << std::endl;
        }
    }
    catch(Exception& e)
    {
        Log::error() << "** " << e.what() << " Caught in " << Here()  <<  std::endl;
        Log::error() << "** Exception is handled" << std::endl;

        throw;
    }

    Log::info() << "Completed index " << path_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
