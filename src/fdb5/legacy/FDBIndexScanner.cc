/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <memory>

#include "eckit/io/StdFile.h"
#include "eckit/io/StdPipe.h"
#include "eckit/utils/Tokenizer.h"
#include "eckit/utils/Translator.h"
#include "eckit/config/Resource.h"
#include "eckit/io/StdFile.h"

#include "fdb5/toc/AdoptVisitor.h"
#include "fdb5/grib/GribDecoder.h"
#include "fdb5/legacy/FDBIndexScanner.h"
#include "fdb5/legacy/LegacyTranslator.h"

#include "metkit/codes/Reader.h"
#include "metkit/codes/Message.h"

using namespace eckit;

namespace fdb5 {
namespace legacy {

//-----------------------------------------------------------------------------

FDBIndexScanner::FDBIndexScanner(const PathName &path, bool compareToGrib, bool checkValues):
    path_(path.realName()),
    compareToGrib_(compareToGrib),
    checkValues_(checkValues)
{
    Log::info() << "FDBIndexScanner( path = " << path_ << ")" << std::endl;
}

FDBIndexScanner::~FDBIndexScanner() {
}

void FDBIndexScanner::execute() {

    Log::info() << "scanning " << path_ << std::endl;

    PathName lsfdb1( path_ + ".lsfdb" );
    PathName lsfdb2( path_ + "lsfdb" );

    if ( lsfdb1.exists() ) {
        Log::info() << "scanning file " << lsfdb1 << std::endl;

        AutoStdFile f( lsfdb1, "r" );
        process(f);
        return;
    }

    if ( lsfdb2.exists() ) {
        Log::info() << "scanning file " << lsfdb2 << std::endl;

        AutoStdFile f( lsfdb2, "r" );
        process(f);
        return;
    }

    // if all else fails, lets try to run the old tool 'lsfdb' (must be in path)
    if (path_.size() != 0) {
        static std::string fdbLegacyLsfdb = eckit::Resource<std::string>("fdbLegacyLsfdb",
                                            "/usr/local/apps/fdb/current/bin/lsfdb");

        std::ostringstream cmd;
        cmd << fdbLegacyLsfdb << " " << path_;

        Log::info() << "scanning with pipe to command: [" << cmd.str() << "]" << std::endl;
        StdPipe pipe(cmd.str(), "r");  AutoCloser<StdPipe> closer(pipe);
        process(pipe);
    }
}

void FDBIndexScanner::process(FILE *f) {
    LegacyTranslator translator;

    std::map<std::string, int> files;
    std::map<eckit::Length, eckit::Length> guess;

    Tokenizer p(":", true);

    std::string ignore;
    std::string s;

    Key r;

    PathName dirpath = path_.dirName();

    // crack the db entry

    StringList db;
    p(dirpath.baseName(), db);

    Log::info() << "Parsed db (size=" << db.size() << ") " << db << std::endl;

    ASSERT(db.size() >= 6);

    translator.set(r, "class",  db[1]);
    translator.set(r, "stream", db[2]);
    translator.set(r, "domain", db[3]);
    translator.set(r, "expver", db[4]);
    translator.set(r, "date",   db[5]);


    Log::info() << path_ << std::endl;

    //     Log::info() << path_.baseName() << std::endl;
    //     Log::info() << std::string(path_.baseName()) << std::endl;

    // /ma_fdb/:od:oper:g:0001:20120617::/:fc:0000::::::::.

    // crack the index filename

    std::string indexFileName(path_.baseName());
    Log::info() << "Index filename [" << indexFileName << "]" << std::endl;

    StringList idx;
    p(indexFileName, idx); // first elem will be always empty

    Log::info() << "Parsed index (size=" << idx.size() << ") " << idx << std::endl;

    ASSERT(idx.size() >= 3);
    if (idx[1] == "tf") {
        return;
    }

    if(idx[1] == "tf") return; // skip tf because FTM we do not handle BUFR type

    translator.set(r, "type",  idx[1]);
    translator.set(r, "time",  idx[2]);

    translator.set(r, "number", idx[3]);

    translator.set(r, "iteration", idx[5]);

    // if r["stream"]
    translator.set(r, "anoffset", idx[8]);

    translator.set(r, "step", "0"); // in case the step is not in the index (eg. type=an)

    try {

        Tokenizer parse("=");
        std::string prefix;

        char buf[10240];

        fgets(buf, sizeof(buf), f);

        while (fgets(buf, sizeof(buf), f)) {
            ASSERT(strlen(buf));

            buf[strlen(buf) - 1] = 0;

            Key key(r);

            std::string line(buf);

            //            Log::info() << "line [" << line << "]" << std::endl;

            if (line.empty()) continue;

            std::istringstream in(line);
            // e.g. 409802 FDB;  4558 0:6:0;  levelist=27 levtype=m parameter=155 step=6  3281188 (61798740)

            in >> ignore;
            in >> ignore;
            in >> ignore;
            in >> prefix;

            prefix = prefix.substr(0, prefix.length() - 1); // chop off trailing ';' so prefix = 0:6:0

            //            Log::info() << "prefix = " << prefix << std::endl;

            std::string datapath = dirpath + "/:" + prefix + path_.baseName(false);
            std::map<std::string, int>::iterator file = files.find(datapath);
            if (file == files.end()) {
                int fd;
                SYSCALL(fd = ::open(datapath.c_str(), O_RDONLY));
                files[datapath] = fd;
                file = files.find(datapath);
            }

            //            Log::info() << "datapath = " << datapath << std::endl;

            Length length;
            Offset offset;
            for (;;) {
                StringList v;
                in >> s;

                //  Log::info() << "s -> [" << s << "]" << std::endl;

                if (isdigit(s[0])) { // when we hit digits, parse length and offset, e.g.   3281188 (61798740)
                    length = Translator<std::string, unsigned long long>()(s);
                    in >> s;
                    offset = Translator<std::string, unsigned long long>()(s.substr(1, s.length() - 2));

                    std::map<eckit::Length, eckit::Length>::const_iterator g = guess.find(length);
                    if (g == guess.end()) {

                        off_t o = off_t(length) + off_t(offset);
                        if (o >= 8) {
                            int fd = (*file).second;

                            o -= 8;
                            SYSCALL(o == lseek(fd, o, SEEK_SET));


                            char sevens[8] = {0,};

                            SYSCALL(::read(fd, sevens, 8));

                            bool ok = false;
                            for (int i = 0; i <= 4 ; i++, o++) {
                                if (sevens[i] == '7' && sevens[i + 1] == '7'
                                        && sevens[i + 2] == '7' && sevens[i + 3] == '7') {
                                    ok = true;
                                    break;
                                }
                            }
                            ASSERT(ok);
                            o += 4;
                        }
                        guess[length] = o - off_t(offset);
                        g = guess.find(length);
                    }
                    length = (*g).second;
                    break;
                }

                // parse keywords, e.g. levelist=27 levtype=m parameter=155 step=6

                parse(s, v);
                ASSERT(v.size() == 2);
                translator.set(key, v[0], v[1]);
            }

//            Log::info() << "Indexed field @ " << datapath << " offset=" << offset << " lenght=" << length << std::endl;


            if (compareToGrib_) {
                metkit::codes::Reader reader(PathName(datapath).partHandle(offset, length));
                GribDecoder decoder;
                Key grib;
                decoder.gribToKey(reader.next(), grib);

                // if(checkValues_) { // FTM we know that param is not comparable in the old FDB
                //     key.set("param", grib.get("param"));
                // }

                try {
                    grib.validateKeysOf(key, checkValues_);
                }
                catch (eckit::Exception& e) {
                    std::cerr << e.what() << std::endl;
                }
            }

            AdoptVisitor visitor(*this, key, datapath, offset, length);

            archive(key, visitor);
        }
    } catch (Exception &e) {
        Log::error() << "** " << e.what() << " Caught in " << Here()  <<  std::endl;
        Log::error() << "** Exception is handled" << std::endl;

        throw;
    }

    Log::info() << "Completed index " << path_ << std::endl;

    for (std::map<std::string, int>::iterator j = files.begin(); j != files.end(); ++j) {
        close((*j).second);
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
