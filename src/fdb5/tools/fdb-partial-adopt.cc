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
#include "eckit/memory/ScopedPtr.h"

#include "fdb5/config/UMask.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Key.h"
#include "fdb5/legacy/LegacyTranslator.h"
#include "fdb5/grib/GribDecoder.h"
#include "fdb5/toc/AdoptVisitor.h"
#include "fdb5/tools/FDBTool.h"

#include "marslib/EmosFile.h"

#include "metkit/MarsLanguage.h"
#include "metkit/MarsRequest.h"

//----------------------------------------------------------------------------------------------------------------------

extern "C" {
    #include "db.h"

    extern fdb_dic* parser_list;
    extern fdb_base *fdbbase;
}

//----------------------------------------------------------------------------------------------------------------------

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBPartialAdopt : public FDBTool {

    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int minimumPositionalArguments() const { return 1; }
    virtual void init(const eckit::option::CmdArgs &args);

    void adoptIndex(const eckit::PathName& indexPath, const Key& request, int* fdb, fdb_base* base);
    Key knodeToKey(dic_grp* g, fdb_knode* knode) const;
    bool partialMatches(const Key& request, const Key& partialkey) const;

    Key gribToKey(const eckit::PathName& datapath, size_t offset, size_t length) const;

  public:

    FDBPartialAdopt(int argc, char **argv) :
        FDBTool(argc, argv),
        fdbName_("fdb"),
        compareToGrib_(false),
        continueOnVerificationError_(false) {
        options_.push_back(new eckit::option::SimpleOption<bool>("verify", "Explicitly verify keys against GRIB headers"));
        options_.push_back(new eckit::option::SimpleOption<bool>("continue", "If a verification error occurs, print but continue"));
    }

  private:
    const std::string fdbName_;

    Archiver archiver_;

    bool compareToGrib_;
    bool continueOnVerificationError_;
};


void FDBPartialAdopt::usage(const std::string &tool) const {
    Log::info() << std::endl
                << "Usage: " << tool << " <request1> [<request2> ...]" << std::endl;
    FDBTool::usage(tool);
}

void FDBPartialAdopt::init(const eckit::option::CmdArgs &args) {
    args.get("verify", compareToGrib_);
    args.get("continue", continueOnVerificationError_);
}


void FDBPartialAdopt::execute(const eckit::option::CmdArgs &args) {

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

    // Iterate over all the requests/keys provided

    for (size_t i = 0; i < args.count(); ++i) {

        // Open the FDB
        // n.b. we do this _inside_ the loop to ensure that we are always operating on a
        //      nice clean fdb4, without any preserved state.

        int fdb;
        if (::openfdb(const_cast<char*>(fdbName_.c_str()), &fdb, const_cast<char*>("r")) != 0) {
            throw FDBToolException("Failed to open fdb", Here());
        }

        fdb_base* base = find_fdbbase(fdbbase, &fdb, const_cast<char*>("FDBPartialAdopt::execute()"));
        ASSERT(base);

        // Iterate over key-value pairs in supplied request

        Key request(args(i));

        // Depending on the type/step, we may have to set the leg. Or iterate over it.

        std::vector<Key> requests;
        if (request.find("type") != request.end() && (request.get("type") == "pf" || request.get("type") == "cf")) {
            if (request.find("step") != request.end()) {
                long s = ::strtol(request.get("step").c_str(), NULL, 10);
                ASSERT(s >= 0);
                if (s < 360) {
                    request.set("leg", "1");
                } else {
                    request.set("leg", "2");
                }
                requests.push_back(request);
            } else {
                for (const std::string& l : {"1", "2"}) {
                    request.set("leg", l);
                    requests.push_back(request);
                }
            }
        } else {
            requests.push_back(request);
        }

        // Loop over the requests determined (normally just one).

        for (const auto& rq : requests) {

            Log::info() << "Rq: " << rq << std::endl;

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

            adoptIndex(index_name, rq, &fdb, base);
        }

        ::closefdb(&fdb);
    }
}


void FDBPartialAdopt::adoptIndex(const PathName &indexPath, const Key &request, int* fdb, fdb_base* base) {

    /*
     * This routine is a little bit ... opaque.
     * It is derived from the code of the lsfdb utility, which is equally opaque.
     */

    legacy::LegacyTranslator translator;

    metkit::MarsLanguage language("retrieve");

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

                        // Obtain filesystem location info for data

                        ::setvalfdb_i(fdb, const_cast<char*>("frank"), knode->rank);
                        ::setvalfdb_i(fdb, const_cast<char*>("fthread"), knode->thread);

                        base->proc->infnam(base);
//                      (alternative)  FdbInFnamNoCache(base);

                        eckit::PathName datapath(base->list->PthNode->name);
                        size_t offset = knode->addr;
                        size_t length = knode->length;

                        // Construct the full key for the match

                        Key fullKey;
                        for (const auto& kv : request) translator.set(fullKey, kv.first, kv.second);
                            for (const auto& kv : partialFieldKey) translator.set(fullKey, kv.first, kv.second);

                        // Convert the Key into a request to pass through the MARS expander

                        metkit::MarsRequest mars_request("retrieve");
                        for (const auto& kv : fullKey) mars_request.setValue(kv.first, kv.second);

                        metkit::MarsRequest expanded_request;
                        try {
                            expanded_request = language.expand(mars_request, false);
                        } catch (...) {
                            Log::info() << "Request: " << mars_request << std::endl;
                            Log::info() << "GRIB: " << gribToKey(datapath, offset, length) << std::endl;
                            throw;
                        }

                        // And convert back into a Key for the FDB5

                        Key archiveKey;
                        for (const auto& p : expanded_request.params()) {
                            std::vector<std::string> values;
                            expanded_request.getValues(p, values);
                            ASSERT(values.size() == 1);
                            archiveKey.set(p, values[0]);
                        }

                        // Leg is a weird one
                        if (archiveKey.find("leg") != archiveKey.end()) archiveKey.unset("leg");

                        // Validate if required

                        if (compareToGrib_) {

                            Key gribKey(gribToKey(datapath, offset, length));

                            // Throws exception on failure
                            try {
                                gribKey.validateKeysOf(archiveKey, true);
                            } catch (Exception& e) {
                                if (!continueOnVerificationError_) throw;
                                Log::error() << e.what() << std::endl;
                            }
                        }

                        // Adopt to FDB5

                        AdoptVisitor visitor(archiver_, archiveKey, datapath, offset, length);
                        archiver_.archive(archiveKey, visitor);
                    }
                }
            }
            g = g->next;
        }
    }
}


Key FDBPartialAdopt::knodeToKey(dic_grp* g, fdb_knode* knode) const {

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
                    if (all_null(pvalue, sizeof(double))) {
                        if (std::string("step") != pattr->name) break;
                    }
                    double intpart;
                    if (std::modf(*reinterpret_cast<const double*>(pvalue), &intpart) == 0.0) {
                        value = Translator<long, std::string>()(static_cast<long>(*reinterpret_cast<const double*>(pvalue)));
                    } else {
                        value = Translator<double, std::string>()(*reinterpret_cast<const double*>(pvalue));
                    }
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


bool FDBPartialAdopt::partialMatches(const Key& request, const Key& partialKey) const {

    for (Key::const_iterator pit = partialKey.begin(); pit != partialKey.end(); ++pit) {

        Key::const_iterator rit = request.find(pit->first);
        if (rit != request.end()) {
            if (pit->second != rit->second) return false;
        }
    }

    return true;
}


Key FDBPartialAdopt::gribToKey(const eckit::PathName& datapath, size_t offset, size_t length) const {

     eckit::ScopedPtr<DataHandle> h(PathName(datapath).partHandle(offset, length));

     EmosFile file(*h);
     GribDecoder decoder;
     Key gribKey;

     decoder.gribToKey(file, gribKey);
     return gribKey;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5


int main(int argc, char **argv) {
    fdb5::FDBPartialAdopt app(argc, argv);
    return app.start();
}

