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
}

void FDBTool::usage(const std::string &tool) {
}


eckit::PathName FDBTool::databasePath(const std::string &arg) const {

    eckit::PathName path(arg);
    if (!path.exists()) {
        const Schema& schema = MasterConfig::instance().schema();

        std::ostringstream oss;
        eckit::Date date(0);

        date -= 1; // Yesterday

        oss << "class=od,expver=0001,domain=g,stream=oper,time=1200,date=";
        oss << date.yyyymmdd();
        oss << "," << arg;

        try {
            Key key(oss.str());
            Key result;

            if (schema.expandFirstLevel(key, result)) {
                path = TocDB::directory(result);
                // Log::info() << arg << " => " << key << " => " << result << " => " << path << std::endl;
            }
        } catch (eckit::Exception& e) {

        }
    }

    if (path.exists()) {

        if (!path.isDir()) {
            path = path.dirName();
        }

        path = path.realName();
    }



    return path;

}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

