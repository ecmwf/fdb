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

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDBTool::FDBTool(int argc, char **argv) : eckit::Tool(argc, argv) {

    options_.push_back(new eckit::option::SimpleOption<std::string>("class", "keyword class"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("stream", "keyword stream"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "keyword expver"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("date", "keyword date"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("time", "keyword time"));
    options_.push_back(new eckit::option::SimpleOption<std::string>("domain", "keyword domain"));

}

void FDBTool::usage(const std::string &tool) {
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

