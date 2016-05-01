/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/tools/FDBTool.h"
#include "fdb5/toc/TocDB.h"
#include "fdb5/database/WriteVisitor.h"

#include "fdb5/rules/Schema.h"
#include "fdb5/config/MasterConfig.h"

#include "eckit/option/Option.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/types/Date.h"
#include "eckit/utils/Translator.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDBTool::FDBTool(int argc, char **argv) : eckit::Tool(argc, argv, "DHSHOME") {

    options_.push_back(new eckit::option::SimpleOption<std::string>("request",  "keyword class"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("stream", "keyword stream"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "keyword expver"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("date",   "keyword date"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("time",   "keyword time"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("domain", "keyword domain"));
}

void FDBTool::usage(const std::string &tool) {
}

eckit::PathName FDBTool::directoryFromOption(const eckit::option::CmdArgs &args) const {
    std::string request;
    args.get("request", request);

    Key k("class=od,expver=0001,date=0,domain=g,stream=oper," + request);


    Log::info() << "Selected Key " << k << std::endl;

    return TocDB::directory(k);
}

eckit::PathName FDBTool::expand(const std::string &arg) const {
    Key key("class=od,expver=0001,date=0,domain=g,stream=oper," + arg);

    struct V : public WriteVisitor {
        V(std::vector<Key>& keys): WriteVisitor(keys) {}
        virtual bool selectDatabase(const Key &key, const Key &full) { std::cout << key << std::endl; return true;}
        virtual bool selectIndex(const Key &key, const Key &full) {return true;}
        virtual bool selectDatum(const Key &key, const Key &full) {return true;}
        virtual void print( std::ostream &out ) const {}
    };

    std::vector<Key> keys(3);
    V visitor(keys);
    MasterConfig::instance().schema().expand(key, visitor);

    eckit::PathName path = TocDB::directory(key);

    Log::info() << arg << " => " << key << " => " << path << std::endl;
    return path;

}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

