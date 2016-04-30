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

#include "eckit/option/Option.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/CmdArgs.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDBTool::FDBTool(int argc, char **argv) : eckit::Tool(argc, argv, "DHSHOME") {

    options_.push_back(new eckit::option::SimpleOption<std::string>("class",  "keyword class"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("stream", "keyword stream"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "keyword expver"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("date",   "keyword date"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("time",   "keyword time"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("domain", "keyword domain"));
}

void FDBTool::usage(const std::string &tool) {
}

eckit::PathName FDBTool::directoryFromOption(const eckit::option::CmdArgs& args) const
{
    std::string kclass("od");
    args.get("class",kclass);
    std::string kstream("oper");
    args.get("stream",kstream);
    std::string kexpver("0001");
    args.get("expver",kexpver);
    std::string kdate("10111977"); /// @todo make equivalent to date=-1 in mars
    args.get("date",kdate);
    std::string ktime("1200");
    args.get("time",ktime);
    std::string kdomain("g");
    args.get("domain",kdomain);

    Key k;
    k.push("class",kclass);
    k.push("expver",kexpver);
    k.push("stream",kstream);
    k.push("date",kdate);
    k.push("time",ktime);
    k.push("domain",kdomain);

    /// @todo come back here: should use schema to get directory path: valuesToString
    ///       uses the order we just pushed in the lines above

    /// @todo It also does not take prepend the FDB root according to the rootd table

    Log::info() << "Selected Key " << k << std::endl;
    Log::info() << "Selected Key str" << k.valuesToString() << std::endl;

    return eckit::PathName(k.valuesToString());
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

