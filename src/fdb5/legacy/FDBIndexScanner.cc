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

#include "fdb5/AdoptVisitor.h"
#include "fdb5/Key.h"

#include "fdb5/legacy/FDBIndexScanner.h"
#include "fdb5/legacy/LegacyTranslator.h"

using namespace eckit;

namespace fdb5 {
namespace legacy {

//-----------------------------------------------------------------------------

FDBIndexScanner::FDBIndexScanner(const PathName& path):
    path_(path.realName())
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
    LegacyTranslator translator;

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

     Log::info() << "CCCCCCC(" << c.size() << ") " << c << std::endl;

    ASSERT(c.size() >= 3);

    Key r;
    translator.set(r, "type",  c[0]);
    translator.set(r, "time",  c[1]);
//    translator.set(r, "number", "0");

    translator.set(r, "step", "0");

    if(c.size() > 4 && !c[4].empty()) {
    	translator.set(r, "iteration", c[4]);
    }
		

    c.clear();

    // Log::info() << c << std::endl;
    // Log::info() << path_ << std::endl;
    // Log::info() << dirpath << std::endl;
    // Log::info() << dirpath.baseName() << std::endl;
    p(dirpath.baseName(),c);

    Log::info() << "[" << c << "]" << std::endl;

    ASSERT(c.size() >= 5);

    translator.set(r, "class", c[0]);
    translator.set(r, "stream", c[1]);
    translator.set(r, "domain", c[2]);
    translator.set(r, "expver", c[3]);
    translator.set(r, "date", c[4]);

    try {

        Tokenizer parse("=");
        std::string prefix;

        char buf[10240];

        fgets(buf, sizeof(buf), f);

        while(fgets(buf, sizeof(buf), f))
        {
            ASSERT(strlen(buf));

            buf[strlen(buf)-1] = 0;

            Key key(r);

            std::string line(buf);

            Log::info() << "line [" << line << "]" << std::endl;

            if(line.empty()) continue;

            std::istringstream in(line);
            // e.g. 409802 FDB;  4558 0:6:0;  levelist=27 levtype=m parameter=155 step=6  3281188 (61798740)

            in >> ignore;
            in >> ignore;
            in >> ignore;
            in >> prefix;

            prefix = prefix.substr(0, prefix.length()-1); // chop off trailing ';' so prefix = 0:6:0

            Log::info() << "prefix = " << prefix << std::endl;

            std::string datapath = dirpath + "/:" + prefix + path_.baseName(false);

            Log::info() << "datapath = " << datapath << std::endl;

            Length length;
            Offset offset;
            for(;;)
            {
                StringList v;
                in >> s;

                Log::info() << "s -> [" << s << "]" << std::endl;

                if(isdigit(s[0])) // when we hit digits, parse length and offset, e.g.   3281188 (61798740)
                {
                    length = Translator<std::string, unsigned long long>()(s);
                    in >> s;
                    offset = Translator<std::string, unsigned long long>()(s.substr(1,s.length()-2));
                    break;
                }

                // parse keywords, e.g. levelist=27 levtype=m parameter=155 step=6

                parse(s,v);
                ASSERT(v.size() == 2);
                translator.set(key, v[0], v[1]);
            }

            Log::info() << "Indexed field @ " << datapath << " offset=" << offset << " lenght=" << length << std::endl;

            AdoptVisitor visitor(*this, key, datapath, offset, length);

            archive(key, visitor);
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
