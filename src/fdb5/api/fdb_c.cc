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
#include "eckit/message/Message.h"
#include "eckit/runtime/Main.h"

#include "metkit/mars/MarsRequest.h"

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


struct fdb_handle_t : public FDB {
    using FDB::FDB;
};

struct fdb_key_t : public Key {
    using Key::Key;
};

struct fdb_request_t {
public:
    fdb_request_t() : request_(metkit::mars::MarsRequest()) {}
    fdb_request_t(std::string str) {
        request_ = metkit::mars::MarsRequest(str);
    }
    void values(const char* name, const char* values[], int numValues) {
        std::string n(name);
        std::vector<std::string> vv;
        for (int i=0; i<numValues; i++) {
            vv.push_back(std::string(values[i]));
        }
        request_.values(n, vv);
    }
    static fdb_request_t* parse(std::string str) {
        fdb_request_t* req = new fdb_request_t();
        req->request_ = metkit::mars::MarsRequest::parse(str);
        return req;
    }
    const metkit::mars::MarsRequest request() const { return request_; }
private:
    metkit::mars::MarsRequest request_;
};


class ListIteratorHolder {
public:
    ListIteratorHolder(ListIterator&& iter) : iter_(std::move(iter)) {}

    ListIterator& get() { return iter_; }

private:
    ListIterator iter_;
};

struct fdb_listiterator_t {
public:
    fdb_listiterator_t() : str_(nullptr), strLength_(0), iter_(nullptr) {}

    void set(ListIterator&& iter) {
        if (iter_)
            delete iter_;

        iter_ = new ListIteratorHolder(std::move(iter));
    }

    bool next(const char** str) {
        if (iter_ == nullptr)
            return false;

        ListElement* el = new ListElement();
        bool exist = iter_->get().next(*el);

        if (exist) {
            std::stringstream ss;
            ss << *el;
            std::string s = ss.str();

            if (str_ == nullptr || strLength_ < s.length()+1) {
                strLength_ = s.length()+20;
                str_ = (char*) realloc(str_, strLength_);
            }
            strcpy(str_, s.c_str());
            (*str) = str_;
        }
        delete(el);
        return exist;
    }

private:
    char* str_;
    size_t strLength_;
    ListIteratorHolder* iter_;
};

struct fdb_datareader_t {
public:
    long open() {
        ASSERT(dh_);
        return dh_->openForRead();
    }
    void close() {
        ASSERT(dh_);
        dh_->close();
    }
    long tell() {
        ASSERT(dh_);
        return dh_->position();
    }
    long seek(long pos) {
        ASSERT(dh_);
        return dh_->seek(pos);
    }
    void skip(long pos) {
        ASSERT(dh_);
        dh_->skip(pos);
    }
    long read(void* buf, long length) {
        ASSERT(dh_);
        return dh_->read(buf, length);
    }
    void set(DataHandle* dh) {
        if (dh_)
            delete dh_;
        dh_ = dh;
    }
    
private:
    DataHandle* dh_;
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

// TODO: In a sensible, templated, try catch all exceptions.
//       --> We ought to have a standardised error return process.


/*
 * Initialise API
 * @note This is only required if being used from a context where Main()
 *       is not otherwise initialised
*/
int fdb_initialise() {
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


int fdb_new_handle(fdb_handle_t** fdb) {
    return wrapApiFunction([fdb] {
        *fdb = new fdb_handle_t();
    });
}

int fdb_archive(fdb_handle_t* fdb, fdb_key_t* key, const char* data, size_t length) {
    return wrapApiFunction([fdb, key, data, length] {
        ASSERT(fdb);
        ASSERT(key);
        ASSERT(data);

        fdb->archive(*key, data, length);
    });
}
int fdb_multi_archive(fdb_handle_t* fdb, const char* data, size_t length) {
    return wrapApiFunction([fdb, data, length] {
        ASSERT(fdb);
        ASSERT(data);

        fdb->archive(data, length);
    });
}
int fdb_archive_and_check(fdb_handle_t* fdb, fdb_request_t* req, const char* data, size_t length) {
    return wrapApiFunction([fdb, req, data, length] {
        ASSERT(fdb);
        ASSERT(req);
        ASSERT(data);

        eckit::MemoryHandle handle(data, length);

        fdb->archive(req->request(), handle);
    });
}

int fdb_list(fdb_handle_t* fdb, const fdb_request_t* req, fdb_listiterator_t* it) {
    return wrapApiFunction([fdb, req, it] {
        ASSERT(fdb);
        ASSERT(it);

        std::vector<std::string> minKeySet; // we consider an empty set
        const FDBToolRequest toolRequest(
            req ? req->request() : metkit::mars::MarsRequest(),
            req == nullptr, minKeySet);

        it->set(fdb->list(toolRequest));
    });
}
int fdb_retrieve(fdb_handle_t* fdb, fdb_request_t* req, fdb_datareader_t* dr) {
    return wrapApiFunction([fdb, req, dr] {
        ASSERT(fdb);
        ASSERT(req);
        ASSERT(dr);
        dr->set(fdb->retrieve(req->request()));
    });
}
int fdb_flush(fdb_handle_t* fdb) {
    return wrapApiFunction([fdb] {
        ASSERT(fdb);

        fdb->flush();
    });
}

int fdb_delete_handle(fdb_handle_t* fdb) {
    return wrapApiFunction([fdb]{
        ASSERT(fdb);
        delete fdb;
    });
}

/** ancillary functions for creating/destroying FDB objects */

int fdb_new_key(fdb_key_t** key) {
    return wrapApiFunction([key] {
        *key = new fdb_key_t();
    });
}
int fdb_key_add(fdb_key_t* key, const char* param, const char* value) {
    return wrapApiFunction([key, param, value]{
        ASSERT(key);
        ASSERT(param);
        ASSERT(value);
        key->set(std::string(param), std::string(value));
    });
}
int fdb_delete_key(fdb_key_t* key) {
    return wrapApiFunction([key]{
        ASSERT(key);
        delete key;
    });
}

int fdb_new_request(fdb_request_t** req) {
    return wrapApiFunction([req] {
        *req = new fdb_request_t("retrieve");
    });
}
int fdb_request_add(fdb_request_t* req, const char* param, const char* values[], int numValues) {
    return wrapApiFunction([req, param, values, numValues] {
        ASSERT(req);
        ASSERT(param);
        ASSERT(values);
        req->values(param, values, numValues);
    });
}
int fdb_delete_request(fdb_request_t* req) {
    return wrapApiFunction([req]{
        ASSERT(req);
        delete req;
    });
}

int fdb_new_listiterator(fdb_listiterator_t** it) {
    return wrapApiFunction([it]{
        *it = new fdb_listiterator_t();
    });
}
int fdb_listiterator_next(fdb_listiterator_t* it, bool* exist, const char** str) {
    return wrapApiFunction([it, exist, str] {
        ASSERT(it);
        ASSERT(exist);
        ASSERT(str);
        *exist = it->next(str);
    });
}
int fdb_delete_listiterator(fdb_listiterator_t* it) {
    return wrapApiFunction([it]{
        ASSERT(it);
        delete it;
    });
}

int fdb_new_datareader(fdb_datareader_t** dr) {
    return wrapApiFunction([dr]{
        *dr = new fdb_datareader_t();
    });
}
int fdb_datareader_open(fdb_datareader_t* dr, long* size) {
    return wrapApiFunction([dr, size]{
        ASSERT(dr);
        long tmp;
        tmp = dr->open();
        if (size)
            *size = tmp;
    });
}
int fdb_datareader_close(fdb_datareader_t* dr) {
    return wrapApiFunction([dr]{
        ASSERT(dr);
        dr->close();
    });
}
int fdb_datareader_tell(fdb_datareader_t* dr, long* pos) {
    return wrapApiFunction([dr, pos]{
        ASSERT(dr);
        ASSERT(pos);
        *pos = dr->tell();
    });
}
int fdb_datareader_seek(fdb_datareader_t* dr, long pos) {
    return wrapApiFunction([dr, pos]{
        ASSERT(dr);
        dr->seek(pos);
    });
}
int fdb_datareader_skip(fdb_datareader_t* dr, long count) {
    return wrapApiFunction([dr, count]{
        ASSERT(dr);
        dr->skip(count);
    });
}
int fdb_datareader_read(fdb_datareader_t* dr, void *buf, long count, long* read) {
    return wrapApiFunction([dr, buf, count, read]{
        ASSERT(dr);
        ASSERT(buf);
        ASSERT(read);
        *read = dr->read(buf, count);
    });
}
int fdb_delete_datareader(fdb_datareader_t* dr) {
    return wrapApiFunction([dr]{
        ASSERT(dr);
        dr->set(nullptr);
        delete dr;
    });
}

//----------------------------------------------------------------------------------------------------------------------

} // extern "C"
