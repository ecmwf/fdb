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
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/AdoptVisitor.h"
#include "fdb5/tools/FDBTool.h"

//----------------------------------------------------------------------------------------------------------------------

extern "C" {
    #include "db.h"

    extern fdb_dic* parser_list;
    extern fdb_base *fdbbase;
}

//----------------------------------------------------------------------------------------------------------------------

// Mapping between param <--> parameter for fdb4/5 for certain stream/type combinations
//
// This is the inverse mapping to the one in pgen/sources/FDB4Sourc..cc

typedef std::map<std::string, std::string> M;
typedef std::map<std::string, M> LookupMap;
const static LookupMap PARAMETER_MAP {

    { "efhs=taem", M{
            {"144", "172144"},
            {"189", "172189"},
            {"228", "172228"},
        }
    },
    { "enfh=fcmean", M{
            {"144", "172144"},
            {"189", "172189"},
            {"228", "172228"},
        }
    },
    { "enfo=efi", M{
            {"44", "132044"},
            {"49", "132049"},
            {"59", "132059"},
            {"144", "132144"},
            {"165", "132165"},
            {"167", "132167"},
            {"201", "132201"},
            {"202", "132202"},
            {"228", "132228"},
        }
    },
    { "enfo=efic", M{
            {"44", "132044"},
            {"49", "132049"},
            {"59", "132059"},
            {"144", "132144"},
            {"165", "132165"},
            {"167", "132167"},
            {"201", "132201"},
            {"202", "132202"},
            {"228", "132228"},
        }
    },
    { "enfo=ep", M{
            {"1", "131001"},
            {"2", "131002"},
            {"3", "131003"},
            {"4", "131004"},
            {"5", "131005"},
            {"6", "131006"},
            {"7", "131007"},
            {"8", "131008"},
            {"10", "131010"},
            {"20", "131020"},
            {"21", "131021"},
            {"22", "131022"},
            {"23", "131023"},
            {"24", "131024"},
            {"25", "131025"},
            {"60", "131060"},
            {"61", "131061"},
            {"62", "131062"},
            {"63", "131063"},
            {"64", "131064"},
            {"65", "131065"},
            {"66", "131066"},
            {"67", "131067"},
            {"68", "131068"},
            {"69", "131069"},
            {"70", "131070"},
            {"71", "131071"},
            {"72", "131072"},
            {"73", "131073"},
            {"89", "131089"},
            {"90", "131090"},
            {"91", "131091"},
        }
    },
    { "enfo=fcmean", M{
            {"138", "171138"},
            {"155", "171155"},
            {"144", "172144"},
            {"189", "172189"},
            {"228", "172228"},
        }
    },
    { "enfo=pd", M{
            {"129", "131129"},
            {"167", "131167"},
            {"228", "131228"},
        }
    },
    { "enfo=sot", M{
            {"44", "132044"},
            {"49", "132049"},
            {"59", "132059"},
            {"144", "132144"},
            {"165", "132165"},
            {"167", "132167"},
            {"201", "132201"},
            {"202", "132202"},
            {"228", "132228"},
        }
    },
    { "enfo=taem", M{
            {"144", "172144"},
            {"189", "172189"},
            {"228", "172228"},
        }
    },
    { "enwh=pf", M{
            {"220", "140220"},
        }
    },
    { "esmm=em", M{
            {"34", "171034"},
            {"129", "171129"},
            {"130", "171130"},
            {"131", "171131"},
            {"132", "171132"},
            {"151", "171151"},
            {"167", "171167"},
            {"228", "171228"},
        }
    },
    { "mmsa=em", M{
            {"33", "171033"},
            {"34", "171034"},
            {"51", "171051"},
            {"52", "171052"},
            {"129", "171129"},
            {"130", "171130"},
            {"138", "171138"},
            {"139", "171139"},
            {"141", "171141"},
            {"151", "171151"},
            {"155", "171155"},
            {"165", "171165"},
            {"166", "171166"},
            {"167", "171167"},
            {"168", "171168"},
            {"144", "173144"},
            {"228", "173228"},
        }
    },
    { "mmsa=fcmean", M{
            {"33", "171033"},
            {"34", "171034"},
            {"39", "171039"},
            {"40", "171040"},
            {"41", "171041"},
            {"42", "171042"},
            {"49", "171049"},
            {"51", "171051"},
            {"52", "171052"},
            {"60", "171060"},
            {"129", "171129"},
            {"130", "171130"},
            {"133", "171133"},
            {"138", "171138"},
            {"139", "171139"},
            {"141", "171141"},
            {"151", "171151"},
            {"155", "171155"},
            {"164", "171164"},
            {"165", "171165"},
            {"166", "171166"},
            {"167", "171167"},
            {"168", "171168"},
            {"169", "171169"},
            {"170", "171170"},
            {"186", "171186"},
            {"207", "171207"},
            {"142", "173142"},
            {"143", "173143"},
            {"144", "173144"},
            {"146", "173146"},
            {"178", "173178"},
            {"179", "173179"},
            {"180", "173180"},
            {"181", "173181"},
            {"182", "173182"},
            {"189", "173189"},
            {"228", "173228"},
            {"147", "173147"},
            {"175", "173175"},
            {"176", "173176"},
            {"177", "173177"},
        }
    },
    { "msmm=em", M{
            {"142", "172142"},
            {"143", "172143"},
            {"144", "172144"},
            {"146", "172146"},
            {"147", "172147"},
            {"178", "172178"},
            {"179", "172179"},
            {"180", "172180"},
            {"181", "172181"},
            {"182", "172182"},
            {"189", "172189"},
            {"228", "172228"},
            {"175", "172175"},
            {"176", "172176"},
            {"177", "172177"},
        }
    },
    { "msmm=fcmean", M{
            {"215", "172140215"}, // TODO: Is this really correct?
            {"142", "172142"},
            {"143", "172143"},
            {"144", "172144"},
            {"146", "172146"},
            {"147", "172147"},
            {"216", "17216"},
            {"178", "172178"},
            {"179", "172179"},
            {"180", "172180"},
            {"181", "172181"},
            {"182", "172182"},
            {"189", "172189"},
            {"228", "172228"},
            {"175", "172175"},
            {"176", "172176"},
            {"177", "172177"},
        }
    },
    { "oper=fc", M{
            {"41", "3041"},
        }
    },
    { "scwv=an", M{
            {"112", "140112"},
            {"113", "140113"},
            {"114", "140114"},
            {"115", "140115"},
            {"116", "140116"},
            {"117", "140117"},
            {"118", "140118"},
            {"119", "140119"},
            {"220", "140220"},
            {"121", "140121"},
            {"122", "140122"},
            {"123", "140123"},
            {"124", "140124"},
            {"125", "140125"},
            {"126", "140126"},
            {"127", "140127"},
            {"128", "140128"},
            {"129", "140129"},
            {"214", "140214"},
            {"215", "140215"},
            {"216", "140216"},
            {"217", "140217"},
            {"218", "140218"},
            {"219", "140219"},
            {"220", "140220"},
            {"220", "140225"},
            {"221", "140221"},
            {"223", "140223"},
            {"224", "140224"},
            {"225", "140225"},
            {"226", "140226"},
            {"227", "140227"},
            {"229", "140229"},
            {"230", "140230"},
            {"231", "140231"},
            {"232", "140232"},
            {"233", "140233"},
            {"234", "140234"},
            {"235", "140235"},
            {"236", "140236"},
            {"237", "140237"},
            {"238", "140238"},
            {"239", "140239"},
            {"244", "140244"},
            {"245", "140245"},
            {"249", "140249"},
            {"251", "140251"},
            {"253", "140253"},
        }
    },
    { "scwv=fc", M{
            {"112", "140112"},
            {"113", "140113"},
            {"114", "140114"},
            {"115", "140115"},
            {"116", "140116"},
            {"117", "140117"},
            {"118", "140118"},
            {"119", "140119"},
            {"220", "140220"},
            {"121", "140121"},
            {"122", "140122"},
            {"123", "140123"},
            {"124", "140124"},
            {"125", "140125"},
            {"126", "140126"},
            {"127", "140127"},
            {"128", "140128"},
            {"129", "140129"},
            {"214", "140214"},
            {"215", "140215"},
            {"216", "140216"},
            {"217", "140217"},
            {"218", "140218"},
            {"219", "140219"},
            {"220", "140220"},
            {"220", "140225"},
            {"221", "140221"},
            {"223", "140223"},
            {"224", "140224"},
            {"225", "140225"},
            {"226", "140226"},
            {"227", "140227"},
            {"229", "140229"},
            {"230", "140230"},
            {"231", "140231"},
            {"232", "140232"},
            {"233", "140233"},
            {"234", "140234"},
            {"235", "140235"},
            {"236", "140236"},
            {"237", "140237"},
            {"238", "140238"},
            {"239", "140239"},
            {"244", "140244"},
            {"245", "140245"},
            {"249", "140249"},
            {"251", "140251"},
            {"253", "140253"},
        }
    },
    { "waef=cf", M{
            {"112", "140112"},
            {"113", "140113"},
            {"114", "140114"},
            {"115", "140115"},
            {"116", "140116"},
            {"117", "140117"},
            {"118", "140118"},
            {"119", "140119"},
            {"220", "140220"},
            {"121", "140121"},
            {"122", "140122"},
            {"123", "140123"},
            {"124", "140124"},
            {"125", "140125"},
            {"126", "140126"},
            {"127", "140127"},
            {"128", "140128"},
            {"129", "140129"},
            {"214", "140214"},
            {"215", "140215"},
            {"216", "140216"},
            {"217", "140217"},
            {"218", "140218"},
            {"219", "140219"},
            {"220", "140220"},
            {"220", "140225"},
            {"221", "140221"},
            {"223", "140223"},
            {"224", "140224"},
            {"225", "140225"},
            {"226", "140226"},
            {"227", "140227"},
            {"229", "140229"},
            {"230", "140230"},
            {"231", "140231"},
            {"232", "140232"},
            {"233", "140233"},
            {"234", "140234"},
            {"235", "140235"},
            {"236", "140236"},
            {"237", "140237"},
            {"238", "140238"},
            {"239", "140239"},
            {"244", "140244"},
            {"245", "140245"},
            {"249", "140249"},
            {"251", "140251"},
        }
    },
    { "waef=ep", M{
            {"74", "131074"},
            {"75", "131075"},
            {"76", "131076"},
            {"77", "131077"},
            {"78", "131078"},
            {"79", "131079"},
            {"80", "131080"},
            {"81", "131081"},
        }
    },
    { "waef=fc", M{
            {"235", "140235"},
        }
    },
    { "waef=pf", M{
            {"112", "140112"},
            {"113", "140113"},
            {"114", "140114"},
            {"115", "140115"},
            {"116", "140116"},
            {"117", "140117"},
            {"118", "140118"},
            {"119", "140119"},
            {"120", "140120"},
            {"121", "140121"},
            {"122", "140122"},
            {"123", "140123"},
            {"124", "140124"},
            {"125", "140125"},
            {"126", "140126"},
            {"127", "140127"},
            {"128", "140128"},
            {"129", "140129"},
            {"211", "140211"},
            {"212", "140212"},
            {"214", "140214"},
            {"215", "140215"},
            {"216", "140216"},
            {"217", "140217"},
            {"218", "140218"},
            {"219", "140219"},
            {"220", "140220"},
            {"221", "140221"},
            {"223", "140223"},
            {"224", "140224"},
            {"225", "140225"},
            {"226", "140226"},
            {"227", "140227"},
            {"229", "140229"},
            {"230", "140230"},
            {"231", "140231"},
            {"232", "140232"},
            {"233", "140233"},
            {"234", "140234"},
            {"235", "140235"},
            {"236", "140236"},
            {"237", "140237"},
            {"238", "140238"},
            {"239", "140239"},
            {"244", "140244"},
            {"245", "140245"},
            {"246", "140246"},
            {"247", "140247"},
            {"249", "140249"},
            {"251", "140251"},
            {"253", "140253"},
        }
    },
    { "waef=efi", M{
            {"216", "132216"},
        }
    },
    { "waef=sot", M{
            {"216", "132216"},
        }
    },
    { "wave=an", M{
            {"112", "140112"},
            {"113", "140113"},
            {"114", "140114"},
            {"115", "140115"},
            {"116", "140116"},
            {"117", "140117"},
            {"118", "140118"},
            {"119", "140119"},
            {"120", "140120"},
            {"121", "140121"},
            {"122", "140122"},
            {"123", "140123"},
            {"124", "140124"},
            {"125", "140125"},
            {"126", "140126"},
            {"127", "140127"},
            {"128", "140128"},
            {"129", "140129"},
            {"207", "140207"},
            {"211", "140211"},
            {"212", "140212"},
            {"214", "140214"},
            {"215", "140215"},
            {"216", "140216"},
            {"217", "140217"},
            {"218", "140218"},
            {"219", "140219"},
            {"220", "140220"},
            {"221", "140221"},
            {"223", "140223"},
            {"224", "140224"},
            {"225", "140225"},
            {"226", "140226"},
            {"227", "140227"},
            {"229", "140229"},
            {"230", "140230"},
            {"231", "140231"},
            {"232", "140232"},
            {"233", "140233"},
            {"234", "140234"},
            {"235", "140235"},
            {"236", "140236"},
            {"237", "140237"},
            {"238", "140238"},
            {"239", "140239"},
            {"244", "140244"},
            {"245", "140245"},
            {"246", "140246"},
            {"247", "140247"},
            {"249", "140249"},
            {"251", "140251"},
            {"253", "140253"},
        }
    },
    {  "wave=fc", M{
            {"112", "140112"},
            {"113", "140113"},
            {"114", "140114"},
            {"115", "140115"},
            {"116", "140116"},
            {"117", "140117"},
            {"118", "140118"},
            {"119", "140119"},
            {"120", "140120"},
            {"121", "140121"},
            {"122", "140122"},
            {"123", "140123"},
            {"124", "140124"},
            {"125", "140125"},
            {"126", "140126"},
            {"127", "140127"},
            {"128", "140128"},
            {"129", "140129"},
            {"207", "140207"},
            {"211", "140211"},
            {"212", "140212"},
            {"214", "140214"},
            {"215", "140215"},
            {"216", "140216"},
            {"217", "140217"},
            {"218", "140218"},
            {"219", "140219"},
            {"220", "140220"},
            {"221", "140221"},
            {"222", "140222"},
            {"223", "140223"},
            {"224", "140224"},
            {"225", "140225"},
            {"226", "140226"},
            {"227", "140227"},
            {"228", "140228"},
            {"229", "140229"},
            {"230", "140230"},
            {"231", "140231"},
            {"232", "140232"},
            {"233", "140233"},
            {"234", "140234"},
            {"235", "140235"},
            {"236", "140236"},
            {"237", "140237"},
            {"238", "140238"},
            {"239", "140239"},
            {"244", "140244"},
            {"245", "140245"},
            {"249", "140249"},
            {"251", "140251"},
            {"252", "140252"},
            {"253", "140253"},
        }
    },

};

//----------------------------------------------------------------------------------------------------------------------

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBPartialAdopt : public FDBTool {

    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int minimumPositionalArguments() const { return 1; }

    void adoptIndex(const eckit::PathName& indexPath, const Key& request, int* fdb, fdb_base* base);
    Key knodeToKey(dic_grp* g, fdb_knode* knode) const;
    bool partialMatches(const Key& request, const Key& partialkey) const;

    void patchKey(Key& key) const;

  public:

    FDBPartialAdopt(int argc, char **argv) :
        FDBTool(argc, argv),
        fdbName_("fdb") {
    }

  private:
    const std::string fdbName_;

    Archiver archiver_;
};


void FDBPartialAdopt::usage(const std::string &tool) const {
    Log::info() << std::endl
                << "Usage: " << tool << " <request1> [<request2> ...]" << std::endl;
    FDBTool::usage(tool);
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

        Key rq(args(i));
        Log::info() << "Key: " << rq << std::endl;

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

        ::closefdb(&fdb);
    }
}


void FDBPartialAdopt::adoptIndex(const PathName &indexPath, const Key &request, int* fdb, fdb_base* base) {

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

                        ::setvalfdb_i(fdb, const_cast<char*>("frank"), knode->rank);
                        ::setvalfdb_i(fdb, const_cast<char*>("fthread"), knode->thread);


                        base->proc->infnam(base);
//                        FdbInFnamNoCache(base);

//                        Log::info() << "match: " << partialFieldKey << std::endl;
//                        Log::info() << "    file   = " << base->list->PthNode->name << std::endl;
//                        Log::info() << "    offset = " << knode->addr << std::endl;
//                        Log::info() << "    length = " << knode->length << std::endl;

                        eckit::PathName datapath(base->list->PthNode->name);
                        size_t offset = knode->addr;
                        size_t length = knode->length;

                        Key fullKey(request);
                        for (const auto& kv : partialFieldKey) {
                            fullKey.set(kv.first, kv.second);
                        }
                        patchKey(fullKey);

                        AdoptVisitor visitor(archiver_, fullKey, datapath, offset, length);
                        archiver_.archive(fullKey, visitor);
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


bool FDBPartialAdopt::partialMatches(const Key& request, const Key& partialKey) const {

    for (Key::const_iterator pit = partialKey.begin(); pit != partialKey.end(); ++pit) {

        Key::const_iterator rit = request.find(pit->first);
        if (rit != request.end()) {
            if (pit->second != rit->second) return false;
        }
    }

    return true;
}

void FDBPartialAdopt::patchKey(Key& key) const {

    if (key.find("parameter") != key.end()) {
        key.set("param", key.get("parameter"));
        key.unset("parameter");
    }

    // Some parameters are referred to with a 1,2 or 3 digit param, whereas they
    // have (standardised, MARS) parameters in the fdb5. Do the interconversion.

    Key::const_iterator stream = key.find("stream");
    Key::const_iterator type = key.find("type");
    Key::const_iterator param = key.find("param");
    if (stream != key.end() && type != key.end() && param != key.end()) {

        std::string lookup_key = stream->second + "=" + type->second;
        LookupMap::const_iterator lookup = PARAMETER_MAP.find(lookup_key);

        if (lookup != PARAMETER_MAP.end()) {
            M::const_iterator new_param = lookup->second.find(param->second);
            if (new_param != lookup->second.end()) {
                Log::warning() << "Patching FDB4 parameter value "
                               << param->second << " --> " << new_param->second << std::endl;
                key.set("param", new_param->second);
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5


int main(int argc, char **argv) {
    fdb5::FDBPartialAdopt app(argc, argv);
    return app.start();
}

