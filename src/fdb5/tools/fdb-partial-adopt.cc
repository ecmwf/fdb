/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <stdlib.h>
#include <sstream>
#include <sys/stat.h>
#include <fcntl.h>

#include "eckit/exception/Exceptions.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/utils/Translator.h"

#include "fdb5/config/UMask.h"
#include "fdb5/tools/FDBTool.h"
#include "fdb5/database/Key.h"

//----------------------------------------------------------------------------------------------------------------------

extern "C" {
    #include "db.h"

    extern fdb_dic* parser_list;
}

//----------------------------------------------------------------------------------------------------------------------

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBAdopt : public FDBTool {

    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int minimumPositionalArguments() const { return 1; }

    void adoptIndex(const eckit::PathName& indexPath, const Key& request) const;
    Key knodeToKey(dic_grp* g, fdb_knode* knode) const;
    bool partialMatches(const Key& request, const Key& partialkey) const;

  public:

    FDBAdopt(int argc, char **argv) :
        FDBTool(argc, argv),
        fdbName_("fdb") {
    }

  private:
    const std::string fdbName_;
};


void FDBAdopt::usage(const std::string &tool) const {
    FDBTool::usage(tool);
}


void FDBAdopt::execute(const eckit::option::CmdArgs &args) {

    UMask umask(UMask::defaultUMask());

    // Extract the key / request that we are operating on

    Log::info() << "args: " << args << std::endl;
    // TODO: Use a real request. Possibly put in a loop?
    Key rq("");

    // Initialise FDB

    ::putenv(const_cast<char*>("FDB_CONFIG_MODE=standalone"));
    initfdb();

    const char* root = ::getenv("FDB_ROOT");
    if (root == NULL) throw UserError("FDB_ROOT must be configured", Here());

    fdb_filesys* fsys = ::IsROOTconfigured(::getenv("FDB_ROOT"));
    if (fsys == NULL) {
        std::stringstream ss;
        ss << "FDB_ROOT '" << root << "' must be configured";
        throw FDBToolException(ss.str(), Here());
    }

    // Open the FDB

    int fdb;
    if (::openfdb(const_cast<char*>(fdbName_.c_str()), &fdb, const_cast<char*>("r")) != 0) {
        throw FDBToolException("Failed to open fdb", Here());
    }

    // Iterate over all the requests/keys provided

    for (size_t i = 0; i < args.count(); ++i) {

        Key rq(args(i));
        Log::info() << "Key: " << rq << std::endl;

        // Iterate over key-value pairs in supplied request

        for (const auto& kv : rq ) {
            // n.b. const_cast due to (incorrect) non-const arguments in fdb headers
            ::setvalfdb(&fdb, const_cast<char*>(kv.first.c_str()), const_cast<char*>(kv.second.c_str()));
        }

        char index_name[FDB_PATH_MAX];
        char data_name[FDB_PATH_MAX];
        int parallelDB;
        if (::IndexandDataStreamNames(&fdb, index_name, data_name, &parallelDB) != 0) {
            throw FDBToolException("There is no available FDB database with the specified parameters", Here());
        }

        adoptIndex(index_name, rq);
    }

    ::closefdb(&fdb);


    //for (size_t i = 0; i < args.count(); i++) {
    //    eckit::PathName path(args(i));

    //    if (path.isDir()) {

    //        // Adopt FDB4

    //        eckit::Log::info() << "Scanning FDB db " << path << std::endl;

    //        std::vector<eckit::PathName> indexes;

    //        eckit::PathName::match(path / pattern, indexes, true); // match checks that path is a directory

    //        for (std::vector<eckit::PathName>::const_iterator j = indexes.begin(); j != indexes.end(); ++j) {
    //            legacy::FDBIndexScanner scanner(*j, compareToGrib, checkValues);
    //            scanner.execute();
    //        }
    //    }
    //}
}


void FDBAdopt::adoptIndex(const PathName &indexPath, const Key &request) const {

    /*
     * This routine is a little bit ... opaque.
     * It is derived from the code of the lsfdb utility, which is equally opaque.
     */

    /// Open the index file

    int idx_fd = ::open(indexPath.asString().c_str(), O_RDONLY);

    if (idx_fd == -1) {
        std::stringstream ss;
        ss << "Failed to open '" << indexPath << "': (" << errno << ") " << strerror(errno);
        throw ReadError(ss.str(), Here());
    }

    fdb_dic* D = ::Find_Database(parser_list, const_cast<char*>(fdbName_.c_str()));
    dic_grp *g = D->DicAttrGroup;

    ASSERT(D);

    fdb_fnode* f = fdb_infnode_lsfdb(idx_fd, indexPath.size());

    ASSERT(f);

    if (fdb_getcache(f, fdb_r)) {
        while(g) {
            if (g->group == 0) {
                for (int i = 0; i <= f->cbuff->key; i++) {
                    fdb_knode* knode = (f->cbuff->knode + i);

                    Key partialFieldKey(knodeToKey(g, knode));

                    if (partialMatches(request, partialFieldKey)) {
                        Log::info() << "partial " << partialFieldKey << std::endl;
                    }
                }
            }
            g = g->next;
        }
    }
}


Key FDBAdopt::knodeToKey(dic_grp* g, fdb_knode* knode) const {

    Key key;

    dic_par* pattr = g->par;
    while (pattr) {
        if (pattr->status != FDB_IGNORE_ATTRIBUTE) {

            int pos = pattr->posingroup - 1;
            char* pvalue = knode->name + pos;
            std::string value;

            switch (pattr->type) {
                case FDB_VALUE_CHAR:
                    if (all_null(pvalue, (pattr->attrlen <= 0 ? 8 : pattr->attrlen))) break;
                    value = pvalue;
                    break;
                case FDB_VALUE_INT:
                    if (all_null(pvalue, sizeof(long))) {
                        if (std::string("step") != pattr->name) break;
                    }
                    value = Translator<long, std::string>()(*reinterpret_cast<const long*>(pvalue));
                    break;
                case FDB_VALUE_FLOAT:
                    if (all_null(pvalue, sizeof(double))) break;
                    value = Translator<double, std::string>()(*reinterpret_cast<const double*>(pvalue));
                    break;
                default:
                    ASSERT(false);
            };

            if (!value.empty()) key.set(pattr->name, value);
        }

        pattr = pattr->next;
    }

    return key;
}


bool FDBAdopt::partialMatches(const Key& request, const Key& partialKey) const {

    for (Key::const_iterator pit = partialKey.begin(); pit != partialKey.end(); ++pit) {

        Key::const_iterator rit = request.find(pit->first);
        if (rit != request.end()) {
            if (pit->second != rit->second) return false;
        }
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5


int main(int argc, char **argv) {
    fdb5::FDBAdopt app(argc, argv);
    return app.start();
}

