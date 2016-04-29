/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/Index.h"

#include "eckit/config/Resource.h"
#include "eckit/io/FileHandle.h"
#include "eckit/parser/JSON.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/thread/AutoLock.h"

#include "eckit/serialisation/Stream.h"

namespace fdb5 {

//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------
Index::Index(eckit::Stream &s, const eckit::PathName &path) : files_("") {
    NOTIMP;
}

Index::Index(const Key &key, const eckit::PathName &path, Index::Mode mode ) :
    mode_(mode),
    path_(path),
    files_(path.dirName()),
    axes_(),
    key_(key),
    prefix_(key.valuesToString()) {

    eckit::PathName json(jsonFile());

    if ( json.exists() ) {
        eckit::Log::info() << "Load " << json << std::endl;
        std::ifstream f(json.asString().c_str());

        eckit::JSONParser parser(f);

        eckit::Value v = parser.parse();
//        eckit::Log::info() << "JSON: " << v << std::endl;

        prefix_ = v["prefix"].as<std::string>();

        files_.load(v["files"]);
//        eckit::Log::info() << "Files " << files_ << std::endl;

        axes_.load(v["axes"]);
        eckit::Log::info() << "Axis " << axes_ << std::endl;


        if (f.bad()) {
            throw eckit::ReadError(json.asString());
        }

    } else {
                eckit::Log::info() << json << " does not exist" << std::endl;

    }
}

Index::~Index() {
    flush();
}

eckit::PathName Index::jsonFile() const {
    std::string path = path_;
    return path.substr(0, path.length() - path_.extension().length()) + ".json";
}

void Index::encode(eckit::Stream& s) const
{
    s << key_;
    s << prefix_;
    files_.encode(s);
    axes_.encode(s);
}

void Index::flush() {
    if (files_.changed() || axes_.changed()) {

        eckit::PathName json(jsonFile());

        eckit::Log::info() << "Save " << json << std::endl;


        eckit::FileHandle f(json);

        f.openForWrite(0);
        eckit::AutoClose closer(f);

        std::ostringstream os;

        eckit::JSON j(os);

        j.startObject();
        j << "prefix" << prefix_;
        j << "files";
        files_.json(j);
        j << "axes";
        axes_.json(j);
        j.endObject();

        f.write(os.str().c_str(), os.str().size());
    }
}

void Index::deleteFiles(bool doit) const
{
    eckit::Log::info() << "File to remove " << jsonFile() << std::endl;
    if(doit) {  jsonFile().unlink(); }
}

void Index::put(const Key &key, const Index::Field &field) {
    axes_.insert(key);
    put_(key, field);
}


//-----------------------------------------------------------------------------

void Index::Field::load(std::istream &s) {
    std::string spath;
    long long offset;
    long long length;
    s >> spath >> offset >> length;
    path_    = spath;
    offset_  = offset;
    length_  = length;
}

void Index::Field::dump(std::ostream &s) const {
    s << path_ << " " << offset_ << " " << length_;
}

//-----------------------------------------------------------------------------

const Key &Index::key() const {
    return key_;
}

//-----------------------------------------------------------------------------



} // namespace fdb5
