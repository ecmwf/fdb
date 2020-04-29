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

#include "fdb5/api/fdc.h"

using namespace fdb5;
using namespace eckit;

extern "C" {

//----------------------------------------------------------------------------------------------------------------------


struct fdc_t : public FDB {
    using FDB::FDB;
};


struct fdc_Key_t : public Key {
    using Key::Key;
};

struct fdc_MarsRequest_t {
public:
    fdc_MarsRequest_t() : request_(metkit::MarsRequest()) {}
    fdc_MarsRequest_t(std::string str) {
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
    static fdc_MarsRequest_t* parse(std::string str) {
        fdc_MarsRequest_t* req = new fdc_MarsRequest_t();
        req->request_ = metkit::MarsRequest::parse(str);
        return req;
    }
    metkit::MarsRequest request() { return request_; }
private:
    metkit::MarsRequest request_;
};


struct fdc_ToolRequest_t {
public:
    fdc_ToolRequest_t(fdc_MarsRequest_t *req, bool all, fdc_KeySet_t *keySet) {
        std::vector<std::string> minKeySet;
        for (int i=0; i<keySet->numKeys; i++)
            minKeySet.push_back(keySet->keySet[i]);

        request_ = new FDBToolRequest(req->request(), all, minKeySet);
    }
    fdc_ToolRequest_t(char *str, bool all, fdc_KeySet_t *keySet) {
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


struct fdc_ListElement_t : public ListElement {
    using ListElement::ListElement;
};


struct fdc_ListIterator_t {
public:
    fdc_ListIterator_t(std::unique_ptr<APIIteratorBase<ListElement>> iter) : iter_(std::move(iter)) {}

    bool next(ListElement& elem) {return iter_->next(elem);}

private:
    std::unique_ptr<APIIteratorBase<ListElement>> iter_;
};


//----------------------------------------------------------------------------------------------------------------------

/* Error handling */

static std::string g_current_error_str;
static fdc_failure_handler_t g_failure_handler = nullptr;
static void* g_failure_handler_context = nullptr;

const char* fdc_error_string(int err) {
    switch (err) {
    case FDC_SUCCESS:
        return "Success";
    case FDC_ERROR_GENERAL_EXCEPTION:
    case FDC_ERROR_UNKNOWN_EXCEPTION:
        return g_current_error_str.c_str();
//    case FDC_ITERATION_COMPLETE:
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
    return FDC_SUCCESS;
}

template <typename FN>
int wrapApiFunction(FN f) {

    try {
        return innerWrapFn(f);
    } catch (Exception& e) {
        Log::error() << "Caught exception on C-C++ API boundary: " << e.what() << std::endl;
        g_current_error_str = e.what();
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDC_ERROR_GENERAL_EXCEPTION);
        }
        return FDC_ERROR_GENERAL_EXCEPTION;
    } catch (std::exception& e) {
        Log::error() << "Caught exception on C-C++ API boundary: " << e.what() << std::endl;
        g_current_error_str = e.what();
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDC_ERROR_GENERAL_EXCEPTION);
        }
        return FDC_ERROR_GENERAL_EXCEPTION;
    } catch (...) {
        Log::error() << "Caught unknown on C-C++ API boundary" << std::endl;
        g_current_error_str = "Unrecognised and unknown exception";
        if (g_failure_handler) {
            g_failure_handler(g_failure_handler_context, FDC_ERROR_UNKNOWN_EXCEPTION);
        }
        return FDC_ERROR_UNKNOWN_EXCEPTION;
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
int fdc_initialise_api() {
    return wrapApiFunction([] {
        static bool initialised = false;

        if (initialised) {
            Log::warning() << "Initialising FDC library twice" << std::endl;
        }

        if (!initialised) {
            const char* argv[2] = {"fdc-api", 0};
            Main::initialise(1, const_cast<char**>(argv));
            initialised = true;
        }
    });
}


int fdc_set_failure_handler(fdc_failure_handler_t handler, void* context) {
    return wrapApiFunction([handler, context] {
        g_failure_handler = handler;
        g_failure_handler_context = context;
        eckit::Log::info() << "FDC setting failure handler fn." << std::endl;
    });
}

int fdc_version(const char** version) {
    return wrapApiFunction([version]{
        (*version) = fdb5_version_str();
    });
}

int fdc_vcs_version(const char** sha1) {
    return wrapApiFunction([sha1]{
        (*sha1) = fdb5_git_sha1();
    });
}


int fdc_init(fdc_t** fdb) {
    return wrapApiFunction([fdb] {
        *fdb = new fdc_t();
    });
}

int fdc_clean(fdc_t* fdc) {
    return wrapApiFunction([fdc]{
        ASSERT(fdc);
        delete fdc;
    });
}

int fdc_cleanPtr(fdc_t** fdc) {
    return wrapApiFunction([fdc]{
        ASSERT(fdc);
        ASSERT(*fdc);
        delete *fdc;
    });
}


int fdc_list(fdc_ListIterator_t** it, fdc_t* fdc, const fdc_ToolRequest_t* req) {
    return wrapApiFunction([fdc, req, it] {
        ListIterator iter = fdc->list(req->request());

        *it = new fdc_ListIterator_t(iter.move());
    });
}
int fdc_list_clean(fdc_ListIterator_t* it) {
    return wrapApiFunction([it]{
        ASSERT(it);
        delete it;
    });
}
int fdc_list_next(fdc_ListIterator_t* it, bool* exist, fdc_ListElement_t** el) {
    return wrapApiFunction([it, exist, el]{
        *exist = it->next(**el);
    });
}

long fdc_dh_to_stream(DataHandle* dh, void* handle, fdc_stream_write_t write_fn) {

    // Wrap the stream in a DataHandle
    struct WriteStreamDataHandle : public eckit::DataHandle {
        WriteStreamDataHandle(void* handle, fdc_stream_write_t fn) : handle_(handle), fn_(fn), pos_(0) {}
        virtual ~WriteStreamDataHandle() {}
        void print(std::ostream& s) const override { s << "StreamReadHandle(" << fn_ << "(" << handle_ << "))"; }
        Length openForRead() override { NOTIMP; }
        void openForWrite(const Length&) override {}
        void openForAppend(const Length&) override {}
        long read(void*, long) override { NOTIMP; }
        long write(const void* buffer, long length) override {
            long written = fn_(handle_, buffer, length);
            pos_ += written;
            return written;
        }
        Offset position() override { return pos_; }
        void close() override {}

        void* handle_;
        fdc_stream_write_t fn_;
        Offset pos_;
    };

    WriteStreamDataHandle sdh(handle, write_fn);
    sdh.openForWrite(0);
    AutoClose closer(sdh);
    dh->copyTo(sdh);
    return sdh.position();
}

long fdc_dh_to_file_descriptor(DataHandle* dh, int fd) {
    FileDescHandle fdh(fd);
    fdh.openForWrite(0);
    AutoClose closer(fdh);
    dh->copyTo(fdh);
    return fdh.position();
}

long fdc_dh_to_buffer(DataHandle* dh, void* buffer, long length) {
    MemoryHandle mdh(buffer, length);
    mdh.openForWrite(0);
    AutoClose closer(mdh);
    dh->copyTo(mdh);
    return mdh.position();
}

int fdc_retrieve_to_stream(fdc_t* fdc, fdc_MarsRequest_t* req, void* handle, fdc_stream_write_t write_fn, long* bytes_encoded) {
    return wrapApiFunction([fdc, req, handle, write_fn, bytes_encoded] {
        DataHandle* dh = fdc->retrieve(req->request());
        long enc = fdc_dh_to_stream(dh, handle, write_fn);
        if (bytes_encoded)
            *bytes_encoded = enc;
    });
}
int fdc_retrieve_to_file_descriptor(fdc_t* fdc, fdc_MarsRequest_t* req, int fd, long* bytes_encoded) {
    return wrapApiFunction([fdc, req, fd, bytes_encoded] {
        DataHandle* dh = fdc->retrieve(req->request());
        long enc = fdc_dh_to_file_descriptor(dh, fd);
        if (bytes_encoded)
            *bytes_encoded = enc;
    });
}
int fdc_retrieve_to_buffer(fdc_t* fdc, fdc_MarsRequest_t* req, void* buffer, long length, long* bytes_encoded) {
    return wrapApiFunction([fdc, req, buffer, length, bytes_encoded] {
        DataHandle* dh = fdc->retrieve(req->request());
        long enc = fdc_dh_to_buffer(dh, buffer, length);
        if (bytes_encoded)
            *bytes_encoded = enc;
    });
}







/** ancillary functions for creating/destroying FDC objects */


int fdc_Key_clean(fdc_Key_t* key) {
    return wrapApiFunction([key]{
        ASSERT(key);
        delete key;
    });
}

int fdc_KeySet_clean(fdc_KeySet_t* keySet) {
    return wrapApiFunction([keySet]{
        ASSERT(keySet);
        delete keySet;
    });
}
int fdc_MarsRequest_init(fdc_MarsRequest_t** req, char* str) {
    return wrapApiFunction([str, req] {
        *req = new fdc_MarsRequest_t(str);
    });
}
int fdc_MarsRequest_value(fdc_MarsRequest_t* req, char* name, char* values[], int numValues) {
    return wrapApiFunction([name, values, numValues, req] {
        req->values(name, values, numValues);
//        std::cout << req->request() << std::endl;
    });
}

int fdc_MarsRequest_parse(fdc_MarsRequest_t** req, char* str) {
    return wrapApiFunction([str, req] {
        *req = fdc_MarsRequest_t::parse(str);
    });
}
int fdc_MarsRequest_clean(fdc_MarsRequest_t* req) {
    return wrapApiFunction([req]{
        ASSERT(req);
        delete req;
    });
}

int fdc_ToolRequest_init_all(fdc_ToolRequest_t** req, fdc_KeySet_t* keys) {
    return wrapApiFunction([keys, req] {
        *req = new fdc_ToolRequest_t(new fdc_MarsRequest_t(), true, keys);
    });
}
int fdc_ToolRequest_init_str(fdc_ToolRequest_t** req, char *str, fdc_KeySet_t *keys) {
    return wrapApiFunction([str, keys, req] {
        *req = new fdc_ToolRequest_t(str, false, keys);
    });
}
int fdc_ToolRequest_init_mars(fdc_ToolRequest_t** req, fdc_MarsRequest_t* marsReq, fdc_KeySet_t* keys) {
    return wrapApiFunction([marsReq, keys, req] {
        *req = new fdc_ToolRequest_t(marsReq, false, keys);
    });
}
int fdc_ToolRequest_clean(fdc_ToolRequest_t* req) {
    return wrapApiFunction([req]{
        //ASSERT(req);
        if (req)
            delete req;
    });
}

int fdc_ListElement_init(fdc_ListElement_t** el) {
    return wrapApiFunction([el] {
        *el = new fdc_ListElement_t();
    });
}
int fdc_ListElement_str(fdc_ListElement_t* el, char **str) {
    return wrapApiFunction([el, str] {
        std::string s = (((ListElement*)el)->combinedKey().valuesToString());
        *str = (char*) malloc(sizeof(char) * s.length()+1);
        strcpy(*str, s.c_str());
    });
}
int fdc_ListElement_clean(fdc_ListElement_t** el) {
    return wrapApiFunction([el]{
        ASSERT(el);
        ASSERT(*el);
        delete *el;
    });
}

/*
namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDB::FDB(const Config &config) :
    internal_(FDBFactory::instance().build(config)),
    dirty_(false),
    reportStats_(config.getBool("statistics", false)) {}


FDB::~FDB() {
    flush();
    if (reportStats_ && internal_) {
        stats_.report(eckit::Log::info(), (internal_->name() + " ").c_str());
        internal_->stats().report(eckit::Log::info(), (internal_->name() + " internal ").c_str());
    }
}

void FDB::archive(const Key& key, const void* data, size_t length) {
    eckit::Timer timer;
    timer.start();

    internal_->archive(key, data, length);
    dirty_ = true;

    timer.stop();
    stats_.addArchive(length, timer);
}

eckit::DataHandle* FDB::retrieve(const metkit::MarsRequest& request) {

    eckit::Timer timer;
    timer.start();
    eckit::DataHandle* dh = internal_->retrieve(request);
    timer.stop();
    stats_.addRetrieve(dh->estimate(), timer);

    return dh;
}

ListIterator FDB::list(const FDBToolRequest& request) {
    return internal_->list(request);
}

DumpIterator FDB::dump(const FDBToolRequest& request, bool simple) {
    return internal_->dump(request, simple);
}

StatusIterator FDB::status(const FDBToolRequest& request) {
    return internal_->status(request);
}

WipeIterator FDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    return internal_->wipe(request, doit, porcelain, unsafeWipeAll);
}

PurgeIterator FDB::purge(const FDBToolRequest &request, bool doit, bool porcelain) {
    return internal_->purge(request, doit, porcelain);
}

StatsIterator FDB::stats(const FDBToolRequest &request) {
    return internal_->stats(request);
}

ControlIterator FDB::control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers) {
    return internal_->control(request, action, identifiers);
}

const std::string FDB::id() const {
    return internal_->id();
}

FDBStats FDB::stats() const {
    return stats_;
}

FDBStats FDB::internalStats() const {
    return internal_->stats();
}

const std::string& FDB::name() const {
    return internal_->name();
}

const Config &FDB::config() const {
    return internal_->config();
}

void FDB::print(std::ostream& s) const {
    s << *internal_;
}

void FDB::flush() {
    if (dirty_) {

        eckit::Timer timer;
        timer.start();

        internal_->flush();
        dirty_ = false;

        timer.stop();
        stats_.addFlush(timer);
    }
}

bool FDB::dirty() const {
    return dirty_;
}

void FDB::disable() {
    internal_->disable();
}

bool FDB::disabled() const {
    return internal_->disabled();
}

bool FDB::writable() const {
    return internal_->writable();
}

bool FDB::visitable() const {
    return internal_->visitable();
}
//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
*/

//----------------------------------------------------------------------------------------------------------------------

} // extern "C"
