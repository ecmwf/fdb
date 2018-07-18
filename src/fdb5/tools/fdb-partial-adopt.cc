/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>
#include <cmath>
#include <ctype.h>
#include <fcntl.h>
#include <sstream>
#include <stdlib.h>
#include <sys/stat.h>

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
#include "metkit/types/TypeParam.h"

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

    long adoptIndex(const eckit::PathName& indexPath, const Key& request, int* fdb, fdb_base* base);
    Key knodeToKey(dic_grp* g, fdb_knode* knode) const;
    bool partialMatches(const Key& request, const Key& partialkey) const;

    std::vector<Key> expandRequest(Key rq) const;
    Key gribToKey(const eckit::PathName& datapath, size_t offset, size_t length) const;
    Key stringToFDB4Key(const std::string& str_key);

    void patchParam(Key& key) const;

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

    size_t totalAdopted = 0;

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

        Key request(stringToFDB4Key(args(i)));
        Log::info() << "Base request: " << request << std::endl;

        // Depending on the type/step, we may have to set the leg. Or iterate over it.
        // Also, expand any wildcards in the request

        std::vector<Key> requests(expandRequest(request));

        // Loop over the requests determined (normally just one).

        size_t adoptedFromSuppliedRequest = 0;

        for (const auto& rq : requests) {

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

            if (PathName(index_name).exists()) {
                adoptedFromSuppliedRequest += adoptIndex(index_name, rq, &fdb, base);
            }
        }

        ::closefdb(&fdb);

        if (adoptedFromSuppliedRequest == 0) {
            std::stringstream ss;
            ss << "No fields found matching supplied request: " << request;
            throw eckit::UserError(ss.str(), Here());
        }

        totalAdopted += adoptedFromSuppliedRequest;
    }

    Log::info() << "Adopted " << totalAdopted << " field(s)" << std::endl;
}


long FDBPartialAdopt::adoptIndex(const PathName &indexPath, const Key &request, int* fdb, fdb_base* base) {

    /*
     * This routine is a little bit ... opaque.
     * It is derived from the code of the lsfdb utility, which is equally opaque.
     */

    legacy::LegacyTranslator translator;

    metkit::MarsLanguage language("retrieve");

    /// Open the index file

    Log::info() << "Adopting from index: " << indexPath << std::endl;

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

    size_t fieldsAdopted = 0;

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

                        // Leg is a weird one
                        if (fullKey.find("leg") != fullKey.end()) fullKey.unset("leg");

                        patchParam(fullKey);

                        // Validate if required

                        if (compareToGrib_) {

                            Key gribKey(gribToKey(datapath, offset, length));

                            // Throws exception on failure
                            try {
                                gribKey.validateKeysOf(fullKey, true);
                            } catch (Exception& e) {
                                Log::info() << "MARS: " << fullKey << std::endl;
                                Log::info() << "GRIB: " << gribKey << std::endl;
                                if (!continueOnVerificationError_) throw;
                                Log::error() << e.what() << std::endl;
                            }
                        }

                        // Adopt to FDB5

                        Log::info() << "Adopting: " << fullKey << std::endl;
                        AdoptVisitor visitor(archiver_, fullKey, datapath, offset, length);
                        archiver_.archive(fullKey, visitor);
                        fieldsAdopted++;
                    }
                }
            }
            g = g->next;
        }
    }

    return fieldsAdopted;
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

std::vector<Key> FDBPartialAdopt::expandRequest(Key rq) const {

    // Depending on the type/step, we may have to set the leg. Or iterate over it.
    // Also, expand any wildcards in the request

    static std::map<std::string, std::vector<std::string>> availableValues {
        {"type", {"an", "cf", "cm", "cr", "cs", "cv", "efi", "efic", "em",
                  "ep", "es", "fc", "fcmax", "fcmean", "fcmin", "fcstdev",
                  "fp", "pb", "pd", "pf", "sot", "ssd", "taem", "taes",
                  "tf", "wp", }},
    };
    if (availableValues.find("number") == availableValues.end()) {
        for (size_t i = 1; i <= 50; i++) {
            availableValues["number"].push_back(eckit::Translator<long, std::string>()(i));
        }
    }

    std::vector<Key> requests;

    // Recurse and substitute *s

    for (const auto& kv : rq) {

        if (kv.second == "*") {

            auto lookup = availableValues.find(kv.first);
            if (lookup == availableValues.end()) {
                std::stringstream ss;
                ss << "Unsupported wildcard for key \"" << kv.first << "\" in request: " << rq;
                throw UserError(ss.str(), Here());
            }

            for (const std::string& value : lookup->second) {
                rq.set(kv.first, value);
                std::vector<Key>&& substituted(expandRequest(rq));
                requests.insert(requests.end(), std::make_move_iterator(substituted.begin()),
                                                std::make_move_iterator(substituted.end()));
            }
        }
    }

    // If there have been no substitutions (i.e. we are at the leaves), then
    // consider legs

    if (requests.size() == 0) {
        if (rq.find("type") != rq.end() &&
                (rq.get("type") == "pf" || rq.get("type") == "cf")) {

            if (rq.find("step") != rq.end()) {
            long s = ::strtol(rq.get("step").c_str(), NULL, 10);
            ASSERT(s >= 0);
            if (s < 360) {
                rq.set("leg", "1");
            } else {
                rq.set("leg", "2");
            }
            requests.push_back(rq);
            } else {
                for (const std::string& l : {"1", "2"}) {
                    rq.set("leg", l);
                    requests.push_back(rq);
                }
            }
        } else {
            requests.push_back(rq);
        }
    }

    return requests;
}


Key FDBPartialAdopt::gribToKey(const eckit::PathName& datapath, size_t offset, size_t length) const {

     eckit::ScopedPtr<DataHandle> h(PathName(datapath).partHandle(offset, length));

     EmosFile file(*h);
     GribDecoder decoder;
     Key gribKey;

     decoder.gribToKey(file, gribKey);
     return gribKey;
}

Key FDBPartialAdopt::stringToFDB4Key(const std::string& str_key) {

    // The FDB4 has requirements on how keys are presented. We put some
    // effort in to canonicalisation
    // --> When the tool is run with keys set in ECFLOW variables, it
    //     should do what people expect

    // Ensure that things that must be lower case, are.

    std::string r(str_key);
    std::transform(r.begin(), r.end(), r.begin(), ::tolower);

    Key key(r);

    eckit::Translator<std::string, long> s2l;
    eckit::Translator<long, std::string> l2s;

    for (const std::string& k : {"step", "levelist", "iteration"}) {
        if (key.find(k) != key.end()) {
            key.set(k, l2s(s2l(key.get(k))));
        }
    }

    return key;
}

void FDBPartialAdopt::patchParam(Key& key) const {

    static metkit::TypeParam typeParam("param", eckit::Value());

    // Some parameters are referred to with a 1,2 or 3 digit param, whereas they
    // have (standardised, MARS) parameters in the fdb5. Do the interconversion.

    Key::const_iterator stream = key.find("stream");
    Key::const_iterator type = key.find("type");
    Key::const_iterator param = key.find("param");
    if (stream != key.end() && type != key.end() && param != key.end()) {

        long newParam = typeParam.paramToParamid(param->second, stream->second, type->second);

        std::string newParamStr(eckit::Translator<long, std::string>()(newParam));

        if (newParamStr != param->second) {
            Log::warning() << "Patching FDB4 parameter value "
                           << param->second << " --> " << newParamStr << std::endl;
            key.set("param", newParamStr);
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5


int main(int argc, char **argv) {
    fdb5::FDBPartialAdopt app(argc, argv);
    return app.start();
}

