/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/message/Message.h"
#include "eckit/runtime/Main.h"
#include "eckit/utils/Tokenizer.h"

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Key.h"
#include "fdb5/fdb5_version.h"

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
    fdb_request_t(std::string str) { request_ = metkit::mars::MarsRequest(str); }
    size_t values(const char* name, char** values[]) {
        std::string n(name);
        std::vector<std::string> vv = request_.values(name);

        *values = new char*[vv.size()];
        for (size_t i = 0; i < vv.size(); i++) {
            (*values)[i] = new char[vv[i].size() + 1];
            strncpy((*values)[i], vv[i].c_str(), vv[i].size());
            (*values)[i][vv[i].size()] = '\0';
        }
        return vv.size();
    }
    void values(const char* name, const char* values[], int numValues) {
        std::string n(name);
        std::vector<std::string> vv;
        Tokenizer parse("/");

        for (int i = 0; i < numValues; i++) {
            std::vector<std::string> result;
            parse(values[i], result);
            vv.insert(std::end(vv), std::begin(result), std::end(result));
        }
        request_.values(n, vv);
    }
    static fdb_request_t* parse(std::string str) {
        fdb_request_t* req = new fdb_request_t();
        req->request_ = metkit::mars::MarsRequest::parse(str);
        return req;
    }
    void expand() {
        bool inherit = false;
        bool strict = true;
        metkit::mars::MarsExpansion expand(inherit, strict);
        request_ = expand.expand(request_);
    }
    const metkit::mars::MarsRequest request() const { return request_; }

private:

    metkit::mars::MarsRequest request_;
};

struct fdb_split_key_t {
    using value_type = std::array<Key, 3>;

    auto operator=(const value_type& keys) -> fdb_split_key_t& {
        keys_ = &keys;
        level_ = keys_->end();
        return *this;
    }

    auto operator++() -> fdb_split_key_t& {
        /// @todo the following "if" is an unfortunate consequence of a flaw in this iterator
        if (level_ == keys_->end()) {
            level_ = keys_->begin();
            curr_ = level_->begin();
            return *this;
        }
        if (curr_ != level_->end()) {
            ++curr_;
            if (curr_ == level_->end() && level_ != keys_->end() - 1) {
                curr_ = (++level_)->begin();
            }
        }
        return *this;
    }

    int next() {
        ++(*this);
        if (curr_ == level_->end()) {
            return FDB_ITERATION_COMPLETE;
        }
        return FDB_SUCCESS;
    }

    void metadata(const char** k, const char** v, size_t* level) const {
        ASSERT_MSG(keys_, "keys are missing!");

        const auto& [key, val] = *curr_;

        *k = key.c_str();
        *v = val.c_str();

        if (level) {
            *level = level_ - keys_->begin();
        }
    }

private:  // members

    const value_type* keys_{nullptr};

    value_type::const_iterator level_;

    Key::const_iterator curr_;
};

struct fdb_listiterator_t {
public:

    fdb_listiterator_t(ListIterator&& iter) : iter_(std::move(iter)), validEl_(false) {}

    int next() {
        validEl_ = iter_.next(el_);

        return validEl_ ? FDB_SUCCESS : FDB_ITERATION_COMPLETE;
    }

    void attrs(const char** uri, size_t* off, size_t* len) {
        ASSERT(validEl_);

        // guard against negative values
        ASSERT(0 <= el_.offset());
        ASSERT(0 <= el_.length());

        *uri = el_.uri().name().c_str();
        *off = el_.offset();
        *len = el_.length();
    }

    void key(fdb_split_key_t* key) {
        ASSERT(validEl_);
        ASSERT(key);

        *key = el_.keys();
    }

private:

    ListIterator iter_;
    bool validEl_;
    ListElement el_;
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
    long size() {
        ASSERT(dh_);
        return dh_->size();
    }
    void set(DataHandle* dh) {
        delete dh_;
        dh_ = dh;
    }

private:

    DataHandle* dh_;
};

// Wipe iterator
struct fdb_wipe_iterator_t {

    fdb_wipe_iterator_t(WipeIterator&& iter) : iter_(std::move(iter)) {}

    int next(WipeElement& e) { return iter_.next(e) ? FDB_SUCCESS : FDB_ITERATION_COMPLETE; }

private:

    WipeIterator iter_;
};

struct fdb_wipe_element_t {

    fdb_wipe_element_t(WipeElement&& e) : element_(std::move(e)) {}

    const char* c_str() const { return element_.c_str(); }

private:

    WipeElement element_;
};

// Purge iterator
struct fdb_purge_iterator_t {

    fdb_purge_iterator_t(PurgeIterator&& iter) : iter_(std::move(iter)) {}

    int next(PurgeElement& e) { return iter_.next(e) ? FDB_SUCCESS : FDB_ITERATION_COMPLETE; }

private:

    PurgeIterator iter_;
};

struct fdb_purge_element_t {

    fdb_purge_element_t(PurgeElement&& e) : element_(std::move(e)) {}

    const char* c_str() const { return element_.c_str(); }

private:

    PurgeElement element_;
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
        case FDB_ITERATION_COMPLETE:
            return "Iteration complete";
        default:
            return "<unknown>";
    };
}

}  // extern "C"

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
    }
    catch (Exception& e) {
        Log::error() << "Caught exception on C-C++ API boundary: " << e.what() << std::endl;
        g_current_error_str = e.what();
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDB_ERROR_GENERAL_EXCEPTION);
        }
        return FDB_ERROR_GENERAL_EXCEPTION;
    }
    catch (std::exception& e) {
        Log::error() << "Caught exception on C-C++ API boundary: " << e.what() << std::endl;
        g_current_error_str = e.what();
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDB_ERROR_GENERAL_EXCEPTION);
        }
        return FDB_ERROR_GENERAL_EXCEPTION;
    }
    catch (...) {
        Log::error() << "Caught unknown on C-C++ API boundary" << std::endl;
        g_current_error_str = "Unrecognised and unknown exception";
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDB_ERROR_UNKNOWN_EXCEPTION);
        }
        return FDB_ERROR_UNKNOWN_EXCEPTION;
    }
}

}  // namespace

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
    return wrapApiFunction([version] { (*version) = fdb5_version_str(); });
}

int fdb_vcs_version(const char** sha1) {
    return wrapApiFunction([sha1] { (*sha1) = fdb5_git_sha1(); });
}


int fdb_new_handle(fdb_handle_t** fdb) {
    return wrapApiFunction([fdb] { *fdb = new fdb_handle_t(); });
}

int fdb_new_handle_from_yaml(fdb_handle_t** fdb, const char* system_config, const char* user_config) {
    return wrapApiFunction([fdb, system_config, user_config] {
        Config cfg{YAMLConfiguration(std::string(system_config)), YAMLConfiguration(std::string(user_config))};
        cfg.set("configSource", "yaml");
        cfg.expandConfig();
        *fdb = new fdb_handle_t(cfg);
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
int fdb_archive_multiple(fdb_handle_t* fdb, fdb_request_t* req, const char* data, size_t length) {
    return wrapApiFunction([fdb, req, data, length] {
        ASSERT(fdb);
        ASSERT(data);

        eckit::MemoryHandle handle(data, length);
        if (req) {
            fdb->archive(req->request(), handle);
        }
        else {
            fdb->archive(handle);
        }
    });
}

int fdb_list(fdb_handle_t* fdb, const fdb_request_t* req, fdb_listiterator_t** it, const bool duplicates, int depth) {
    return wrapApiFunction([fdb, req, it, duplicates, depth] {
        ASSERT(fdb);
        ASSERT(it);
        int d = depth;
        if (depth < 1 || 3 < depth) {
            Log::warning() << "Invalid value depth=" << depth << " - setting depth=3" << std::endl;
            d = 3;
        }

        std::vector<std::string> minKeySet;  // we consider an empty set
        const FDBToolRequest toolRequest(req ? req->request() : metkit::mars::MarsRequest(), req == nullptr, minKeySet);

        *it = new fdb_listiterator_t(fdb->list(toolRequest, duplicates, d));
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

// ---------------------------------------------------------------
// Wipe
// ---------------------------------------------------------------

int fdb_wipe(fdb_handle_t* fdb, fdb_request_t* req, bool doit, bool porcelain, bool unsafeWipeAll,
             fdb_wipe_iterator_t** it) {
    return wrapApiFunction([=] {
        ASSERT(fdb);
        ASSERT(req);
        ASSERT(it);

        *it = new fdb_wipe_iterator_t(fdb->wipe(req->request(), doit, porcelain, unsafeWipeAll));
    });
}

int fdb_wipe_iterator_next(fdb_wipe_iterator_t* it, fdb_wipe_element_t** element) {
    return wrapApiFunction(std::function<int()>{[=] {
        ASSERT(it);
        ASSERT(element);

        WipeElement e;
        int ret = it->next(e);
        *element = new fdb_wipe_element_t(std::move(e));
        return ret;
    }});
}

int fdb_wipe_element_string(fdb_wipe_element_t* element, const char** str) {
    return wrapApiFunction([element, str] {
        ASSERT(element);
        ASSERT(str);
        *str = element->c_str();
    });
}

int fdb_delete_wipe_element(fdb_wipe_element_t* element) {
    return wrapApiFunction([element] {
        ASSERT(element);
        delete element;
    });
}

int fdb_delete_wipe_iterator(fdb_wipe_iterator_t* it) {
    return wrapApiFunction([it] { delete it; });
}

// ---------------------------------------------------------------
// Purge
// ---------------------------------------------------------------

int fdb_purge(fdb_handle_t* fdb, fdb_request_t* req, bool doit, bool porcelain, fdb_purge_iterator_t** it) {
    return wrapApiFunction([=] {
        ASSERT(fdb);
        ASSERT(req);
        ASSERT(it);

        *it = new fdb_purge_iterator_t(fdb->purge(req->request(), doit, porcelain));
    });
}

int fdb_purge_iterator_next(fdb_purge_iterator_t* it, fdb_purge_element_t** element) {
    return wrapApiFunction(std::function<int()>{[=] {
        ASSERT(it);
        ASSERT(element);

        PurgeElement e;
        int ret = it->next(e);
        *element = new fdb_purge_element_t(std::move(e));
        return ret;
    }});
}

int fdb_purge_element_string(fdb_purge_element_t* element, const char** str) {
    return wrapApiFunction([element, str] {
        ASSERT(element);
        ASSERT(str);
        *str = element->c_str();
    });
}

int fdb_delete_purge_element(fdb_purge_element_t* element) {
    return wrapApiFunction([element] {
        ASSERT(element);
        delete element;
    });
}

int fdb_delete_purge_iterator(fdb_purge_iterator_t* it) {
    return wrapApiFunction([it] { delete it; });
}

// ------------------------------------------------------------------

int fdb_delete_handle(fdb_handle_t* fdb) {
    return wrapApiFunction([fdb] {
        ASSERT(fdb);
        delete fdb;
    });
}

/** ancillary functions for creating/destroying FDB objects */

int fdb_new_key(fdb_key_t** key) {
    return wrapApiFunction([key] { *key = new fdb_key_t(); });
}
int fdb_key_add(fdb_key_t* key, const char* param, const char* value) {
    return wrapApiFunction([key, param, value] {
        ASSERT(key);
        ASSERT(param);
        ASSERT(value);
        key->set(std::string(param), std::string(value));
    });
}

int fdb_delete_key(fdb_key_t* key) {
    return wrapApiFunction([key] {
        ASSERT(key);
        delete key;
    });
}

int fdb_new_request(fdb_request_t** req) {
    return wrapApiFunction([req] { *req = new fdb_request_t("retrieve"); });
}
int fdb_request_add(fdb_request_t* req, const char* param, const char* values[], int numValues) {
    return wrapApiFunction([req, param, values, numValues] {
        ASSERT(req);
        ASSERT(param);
        ASSERT(values);
        req->values(param, values, numValues);
    });
}
int fdb_request_get(fdb_request_t* req, const char* param, char** values[], size_t* numValues) {
    return wrapApiFunction([req, param, values, numValues] {
        ASSERT(req);
        ASSERT(param);
        *numValues = req->values(param, values);
    });
}
int fdb_expand_request(fdb_request_t* req) {
    return wrapApiFunction([req] { req->expand(); });
}
int fdb_delete_request(fdb_request_t* req) {
    return wrapApiFunction([req] {
        ASSERT(req);
        delete req;
    });
}

int fdb_listiterator_next(fdb_listiterator_t* it) {
    return wrapApiFunction(std::function<int()>{[it] {
        ASSERT(it);
        return it->next();
    }});
}
int fdb_listiterator_attrs(fdb_listiterator_t* it, const char** uri, size_t* off, size_t* len) {
    return wrapApiFunction([it, uri, off, len] {
        ASSERT(it);
        ASSERT(uri);
        ASSERT(off);
        ASSERT(len);
        it->attrs(uri, off, len);
    });
}
int fdb_listiterator_splitkey(fdb_listiterator_t* it, fdb_split_key_t* key) {
    return wrapApiFunction([it, key] {
        ASSERT(it);
        ASSERT(key);
        it->key(key);
    });
}
int fdb_delete_listiterator(fdb_listiterator_t* it) {
    return wrapApiFunction([it] {
        ASSERT(it);
        delete it;
    });
}

int fdb_new_splitkey(fdb_split_key_t** key) {
    return wrapApiFunction([key] { *key = new fdb_split_key_t(); });
}

int fdb_splitkey_next_metadata(fdb_split_key_t* it, const char** key, const char** value, size_t* level) {
    return wrapApiFunction(std::function<int()>{[it, key, value, level] {
        ASSERT(it);
        ASSERT(key);
        ASSERT(value);
        const auto stat = it->next();
        if (stat == FDB_SUCCESS) {
            it->metadata(key, value, level);
        }
        return stat;
    }});
}
int fdb_delete_splitkey(fdb_split_key_t* key) {
    return wrapApiFunction([key] {
        ASSERT(key);
        delete key;
    });
}

int fdb_new_datareader(fdb_datareader_t** dr) {
    return wrapApiFunction([dr] { *dr = new fdb_datareader_t(); });
}
int fdb_datareader_open(fdb_datareader_t* dr, long* size) {
    return wrapApiFunction([dr, size] {
        ASSERT(dr);
        long tmp;
        tmp = dr->open();
        if (size) {
            *size = tmp;
        }
    });
}
int fdb_datareader_close(fdb_datareader_t* dr) {
    return wrapApiFunction([dr] {
        ASSERT(dr);
        dr->close();
    });
}
int fdb_datareader_tell(fdb_datareader_t* dr, long* pos) {
    return wrapApiFunction([dr, pos] {
        ASSERT(dr);
        ASSERT(pos);
        *pos = dr->tell();
    });
}
int fdb_datareader_seek(fdb_datareader_t* dr, long pos) {
    return wrapApiFunction([dr, pos] {
        ASSERT(dr);
        dr->seek(pos);
    });
}
int fdb_datareader_skip(fdb_datareader_t* dr, long count) {
    return wrapApiFunction([dr, count] {
        ASSERT(dr);
        dr->skip(count);
    });
}
int fdb_datareader_read(fdb_datareader_t* dr, void* buf, long count, long* read) {
    return wrapApiFunction([dr, buf, count, read] {
        ASSERT(dr);
        ASSERT(buf);
        ASSERT(read);
        *read = dr->read(buf, count);
    });
}
int fdb_datareader_size(fdb_datareader_t* dr, long* size) {
    return wrapApiFunction([=] {
        ASSERT(dr);
        *size = dr->size();
    });
}
int fdb_delete_datareader(fdb_datareader_t* dr) {
    return wrapApiFunction([dr] {
        ASSERT(dr);
        dr->set(nullptr);
        delete dr;
    });
}

//----------------------------------------------------------------------------------------------------------------------

}  // extern "C"
