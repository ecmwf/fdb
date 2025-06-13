// (C) Copyright 2011-2025 ECMWF.
//
// This software is licensed under the terms of the Apache Licence Version 2.0
// which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
// In applying this licence, ECMWF does not waive the privileges and immunities
// granted to it by virtue of its status as an intergovernmental organisation
// nor does it submit to any jurisdiction.

int fdb_initialise();
int fdb_version(const char** version);
int fdb_vcs_version(const char** version);
enum FdbErrorValues {
    FDB_SUCCESS                  = 0,
    FDB_ERROR_GENERAL_EXCEPTION  = 1,
    FDB_ERROR_UNKNOWN_EXCEPTION  = 2,
    FDB_ITERATION_COMPLETE       = 3
};
const char* fdb_error_string(int err);
typedef void (*fdb_failure_handler_t)(void* context, int error_code);
int fdb_set_failure_handler(fdb_failure_handler_t handler, void* context);

struct fdb_key_t;
typedef struct fdb_key_t fdb_key_t;
int fdb_new_key(fdb_key_t** key);
int fdb_key_add(fdb_key_t* key, const char* param, const char* value);
int fdb_delete_key(fdb_key_t* key);

struct fdb_request_t;
typedef struct fdb_request_t fdb_request_t;
int fdb_new_request(fdb_request_t** req);
int fdb_request_add(fdb_request_t* req, const char* param, const char* values[], int numValues);
int fdb_expand_request(fdb_request_t* req);
int fdb_delete_request(fdb_request_t* req);

struct fdb_split_key_t;
typedef struct fdb_split_key_t fdb_split_key_t;
int fdb_new_splitkey(fdb_split_key_t** key);
int fdb_splitkey_next_metadata(fdb_split_key_t* it, const char** key, const char** value, size_t* level);
int fdb_delete_splitkey(fdb_split_key_t* key);

struct fdb_listiterator_t;
typedef struct fdb_listiterator_t fdb_listiterator_t;
int fdb_listiterator_next(fdb_listiterator_t* it);
int fdb_listiterator_attrs(fdb_listiterator_t* it, const char** uri, size_t* off, size_t* len);
int fdb_listiterator_splitkey(fdb_listiterator_t* it, fdb_split_key_t* key);
int fdb_delete_listiterator(fdb_listiterator_t* it);

struct fdb_datareader_t;
typedef struct fdb_datareader_t fdb_datareader_t;
int fdb_new_datareader(fdb_datareader_t** dr);
int fdb_datareader_open(fdb_datareader_t* dr, long* size);
int fdb_datareader_close(fdb_datareader_t* dr);
int fdb_datareader_tell(fdb_datareader_t* dr, long* pos);
int fdb_datareader_seek(fdb_datareader_t* dr, long pos);
int fdb_datareader_skip(fdb_datareader_t* dr, long count);
int fdb_datareader_size(fdb_datareader_t* dr, long* size);
int fdb_datareader_read(fdb_datareader_t* dr, void *buf, long count, long* read);
int fdb_delete_datareader(fdb_datareader_t* dr);

struct fdb_handle_t;
typedef struct fdb_handle_t fdb_handle_t;
int fdb_new_handle(fdb_handle_t** fdb);
int fdb_new_handle_from_yaml(fdb_handle_t** fdb, const char* config, const char* user_config);
int fdb_archive(fdb_handle_t* fdb, fdb_key_t* key, const char* data, size_t length);
int fdb_archive_multiple(fdb_handle_t* fdb, fdb_request_t* req, const char* data, size_t length);
int fdb_list(fdb_handle_t* fdb, const fdb_request_t* req, fdb_listiterator_t** it, bool duplicates);
int fdb_retrieve(fdb_handle_t* fdb, fdb_request_t* req, fdb_datareader_t* dr);
int fdb_flush(fdb_handle_t* fdb);
int fdb_delete_handle(fdb_handle_t* fdb);
