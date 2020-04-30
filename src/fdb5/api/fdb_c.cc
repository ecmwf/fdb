/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/MemoryHandle.h"
#include "eckit/io/FileDescHandle.h"
#include "eckit/runtime/Main.h"


#include "metkit/MarsRequest.h"

#include "fdb5/fdb5_version.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Key.h"

#include "fdb5/api/fdb_c.h"

using namespace fdb5;
using namespace eckit;

extern "C" {

//----------------------------------------------------------------------------------------------------------------------


struct fdb_t : public FDB {
    using FDB::FDB;
};

struct fdb_Key_t : public Key {
    using Key::Key;
};

struct fdb_MarsRequest_t {
public:
    fdb_MarsRequest_t() : request_(metkit::MarsRequest()) {}
    fdb_MarsRequest_t(std::string str) {
        request_ = metkit::MarsRequest(str);
    }
    void values(char* name, char* values[], int numValues) {
        std::string n(name);
        std::vector<std::string> vv;
        for (int i=0; i<numValues; i++) {
            vv.push_back(std::string(values[i]));
        }
        request_.values(n, vv);
    }
    static fdb_MarsRequest_t* parse(std::string str) {
        fdb_MarsRequest_t* req = new fdb_MarsRequest_t();
        req->request_ = metkit::MarsRequest::parse(str);
        return req;
    }
    metkit::MarsRequest request() { return request_; }
private:
    metkit::MarsRequest request_;
};


struct fdb_ToolRequest_t {
public:
    fdb_ToolRequest_t(fdb_MarsRequest_t *req, bool all, fdb_KeySet_t *keySet) {
        std::vector<std::string> minKeySet;
        for (int i=0; i<keySet->numKeys; i++)
            minKeySet.push_back(keySet->keySet[i]);

        request_ = new FDBToolRequest(req->request(), all, minKeySet);
    }
    fdb_ToolRequest_t(char *str, bool all, fdb_KeySet_t *keySet) {
        std::vector<std::string> minKeySet;
        for (int i=0; i<keySet->numKeys; i++)
            minKeySet.push_back(keySet->keySet[i]);

        std::vector<FDBToolRequest> requests = FDBToolRequest::requestsFromString(str, minKeySet);
        ASSERT(requests.size() > 0);
        request_ = new FDBToolRequest(requests[0].request(), all, minKeySet);
    }
    const FDBToolRequest& request() const { return *request_; }
private:
    FDBToolRequest* request_;
};


struct fdb_ListElement_t : public ListElement {
    using ListElement::ListElement;
};

struct fdb_ListIterator_t {
public:
    fdb_ListIterator_t(ListIterator&& iter) : iter_(std::move(iter)) {}

    bool next(ListElement& elem) { return iter_.next(elem); }

private:
    ListIterator iter_;
};

struct fdb_DataReader_t : public DataHandle {
    using DataHandle::DataHandle;
};

//----------------------------------------------------------------------------------------------------------------------

/* Error handling */

static std::string g_current_error_str;
static fdb_failure_handler_t g_failure_handler = nullptr;
static void* g_failure_handler_context = nullptr;

const char* fdb_error_string(int err) {
    switch (err) {
    case FDB_SUCCESS:
        return "Success";
    case FDB_ERROR_GENERAL_EXCEPTION:
    case FDB_ERROR_UNKNOWN_EXCEPTION:
        return g_current_error_str.c_str();
//    case FDB_ITERATION_COMPLETE:
//        return "Iteration complete";
    default:
        return "<unknown>";
    };
}

} // extern "C"

// Template can't have C linkage


namespace {

// Template magic to provide a consistent error-handling approach

int innerWrapFn(std::function<int()> f) {
    return f();
}

int innerWrapFn(std::function<void()> f) {
    f();
    return FDB_SUCCESS;
}

template <typename FN>
int wrapApiFunction(FN f) {

    try {
        return innerWrapFn(f);
    } catch (Exception& e) {
        Log::error() << "Caught exception on C-C++ API boundary: " << e.what() << std::endl;
        g_current_error_str = e.what();
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDB_ERROR_GENERAL_EXCEPTION);
        }
        return FDB_ERROR_GENERAL_EXCEPTION;
    } catch (std::exception& e) {
        Log::error() << "Caught exception on C-C++ API boundary: " << e.what() << std::endl;
        g_current_error_str = e.what();
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDB_ERROR_GENERAL_EXCEPTION);
        }
        return FDB_ERROR_GENERAL_EXCEPTION;
    } catch (...) {
        Log::error() << "Caught unknown on C-C++ API boundary" << std::endl;
        g_current_error_str = "Unrecognised and unknown exception";
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDB_ERROR_UNKNOWN_EXCEPTION);
        }
        return FDB_ERROR_UNKNOWN_EXCEPTION;
    }

    ASSERT(false);
}

}

extern "C" {

//----------------------------------------------------------------------------------------------------------------------

// TODO: In a sensible, templated, way catch all exceptions.
//       --> We ought to have a standardised error return process.


/*
 * Initialise API
 * @note This is only required if being used from a context where Main()
 *       is not otherwise initialised
*/
int fdb_initialise_api() {
    return wrapApiFunction([] {
        static bool initialised = false;

        if (initialised) {
            Log::warning() << "Initialising FDB library twice" << std::endl;
        }

        if (!initialised) {
            const char* argv[2] = {"fdb-api", 0};
            Main::initialise(1, const_cast<char**>(argv));
            initialised = true;
        }
    });
}


int fdb_set_failure_handler(fdb_failure_handler_t handler, void* context) {
    return wrapApiFunction([handler, context] {
        g_failure_handler = handler;
        g_failure_handler_context = context;
        eckit::Log::info() << "FDB setting failure handler fn." << std::endl;
    });
}

int fdb_version(const char** version) {
    return wrapApiFunction([version]{
        (*version) = fdb5_version_str();
    });
}

int fdb_vcs_version(const char** sha1) {
    return wrapApiFunction([sha1]{
        (*sha1) = fdb5_git_sha1();
    });
}


int fdb_init(fdb_t** fdb) {
    return wrapApiFunction([fdb] {
        *fdb = new fdb_t();
    });
}

int fdb_clean(fdb_t* fdb) {
    return wrapApiFunction([fdb]{
        ASSERT(fdb);
        delete fdb;
    });
}

int fdb_list(fdb_t* fdb, const fdb_ToolRequest_t* req, fdb_ListIterator_t** it) {
    return wrapApiFunction([fdb, req, it] {
        *it = new fdb_ListIterator_t(fdb->list(req->request()));
    });
}
int fdb_list_clean(fdb_ListIterator_t* it) {
    return wrapApiFunction([it]{
        ASSERT(it);
        delete it;
    });
}
int fdb_list_next(fdb_ListIterator_t* it, bool* exist, fdb_ListElement_t** el) {
    return wrapApiFunction([it, exist, el]{
        *exist = it->next(**el);
    });
}

int fdb_retrieve(fdb_t* fdb, fdb_MarsRequest_t* req, fdb_DataReader_t** dr) {
    return wrapApiFunction([fdb, req, dr] {
        *dr = (fdb_DataReader_t*) fdb->retrieve(req->request());
    });
}


/** ancillary functions for creating/destroying FDB objects */

int fdb_Key_init(fdb_Key_t** key) {
    return wrapApiFunction([key] {
        *key = new fdb_Key_t();
    });
}
int fdb_Key_set(fdb_Key_t* key, char* k, char* v) {
    return wrapApiFunction([key, k, v]{
        key->set(std::string(k), std::string(v));
        std::cout << *((Key*)key) << std::endl;
    });
}


int fdb_Key_clean(fdb_Key_t* key) {
    return wrapApiFunction([key]{
        ASSERT(key);
        delete key;
    });
}

int fdb_KeySet_clean(fdb_KeySet_t* keySet) {
    return wrapApiFunction([keySet]{
        ASSERT(keySet);
        delete keySet;
    });
}
int fdb_MarsRequest_init(fdb_MarsRequest_t** req, char* str) {
    return wrapApiFunction([str, req] {
        *req = new fdb_MarsRequest_t(str);
    });
}
int fdb_MarsRequest_value(fdb_MarsRequest_t* req, char* name, char* values[], int numValues) {
    return wrapApiFunction([name, values, numValues, req] {
        req->values(name, values, numValues);
//        std::cout << req->request() << std::endl;
    });
}

int fdb_MarsRequest_parse(fdb_MarsRequest_t** req, char* str) {
    return wrapApiFunction([str, req] {
        *req = fdb_MarsRequest_t::parse(str);
    });
}
int fdb_MarsRequest_clean(fdb_MarsRequest_t* req) {
    return wrapApiFunction([req]{
        ASSERT(req);
        delete req;
    });
}

int fdb_ToolRequest_init_all(fdb_ToolRequest_t** req, fdb_KeySet_t* keys) {
    return wrapApiFunction([keys, req] {
        *req = new fdb_ToolRequest_t(new fdb_MarsRequest_t(), true, keys);
    });
}
int fdb_ToolRequest_init_str(fdb_ToolRequest_t** req, char *str, fdb_KeySet_t *keys) {
    return wrapApiFunction([str, keys, req] {
        *req = new fdb_ToolRequest_t(str, false, keys);
    });
}
int fdb_ToolRequest_init_mars(fdb_ToolRequest_t** req, fdb_MarsRequest_t* marsReq, fdb_KeySet_t* keys) {
    return wrapApiFunction([marsReq, keys, req] {
        *req = new fdb_ToolRequest_t(marsReq, false, keys);
    });
}
int fdb_ToolRequest_clean(fdb_ToolRequest_t* req) {
    return wrapApiFunction([req]{
        //ASSERT(req);
        if (req)
            delete req;
    });
}

int fdb_ListElement_init(fdb_ListElement_t** el) {
    return wrapApiFunction([el] {
        *el = new fdb_ListElement_t();
    });
}
int fdb_ListElement_str(fdb_ListElement_t* el, char **str) {
    return wrapApiFunction([el, str] {
        std::string s = (((ListElement*)el)->combinedKey().valuesToString());
        *str = (char*) malloc(sizeof(char) * s.length()+1);
        strcpy(*str, s.c_str());
    });
}
int fdb_ListElement_clean(fdb_ListElement_t** el) {
    return wrapApiFunction([el]{
        ASSERT(el);
        ASSERT(*el);
        delete *el;
    });
}

int fdb_DataReader_open(fdb_DataReader_t* dr) {
    return wrapApiFunction([dr]{
        dr->openForRead();
    });
}
int fdb_DataReader_close(fdb_DataReader_t* dr) {
    return wrapApiFunction([dr]{
        dr->close();
    });
}
int fdb_DataReader_tell(fdb_DataReader_t* dr, long* pos) {
    return wrapApiFunction([dr, pos]{
        *pos = dr->position();
    });
}
int fdb_DataReader_seek(fdb_DataReader_t* dr, long pos) {
    return wrapApiFunction([dr, pos]{
        dr->seek(pos);
    });
}
int fdb_DataReader_skip(fdb_DataReader_t* dr, long count) {
    return wrapApiFunction([dr, count]{
        dr->skip(count);
    });
}
int fdb_DataReader_read(fdb_DataReader_t* dr, void *buf, long count, long* read) {
    return wrapApiFunction([dr, buf, count, read]{
        *read = dr->read(buf, count);
    });
}
int fdb_DataReader_saveTo(fdb_DataReader_t* dr, int fd, long* read) {
    return wrapApiFunction([dr, fd, read]{
        FileDescHandle fdh(fd);
        fdh.openForWrite(0);
        AutoClose closer(fdh);
        dr->saveInto(fdh);
        if (read)
            *read = fdh.position();
    });
}

int fdb_DataReader_clean(fdb_DataReader_t* dr) {
    return wrapApiFunction([dr]{
        delete dr;
    });
}

//----------------------------------------------------------------------------------------------------------------------

} // extern "C"
