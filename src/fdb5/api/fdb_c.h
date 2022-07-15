/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/** @author Emanuele Danovaro */
/** @author Simon Smart */
/** @author Tiago Quintino */
/** @date   Apr 2020 */

#ifndef fdb5_api_fdb_c_H
#define fdb5_api_fdb_c_H

#ifdef __cplusplus
extern "C" {
#endif

/*--------------------------------------------------------------------------------------------------------------------*/

#include <stdbool.h>
#include <stddef.h>

/**
 * Initialise API
 * @note This is only required if being used from a context where eckit::Main() is not otherwise initialised
 */

///@{

int fdb_initialise();

///@}


/** Version accessors */

///@{

/** Human readable release version e.g. 1.2.3 */
int fdb_version(const char** version);
/** Version under VCS system, typically a git sha1. Not useful for computing software dependencies. */
int fdb_vcs_version(const char** version);

///@}


/** Error handling */

///@{

enum FdbErrorValues {
    FDB_SUCCESS                  = 0,
    FDB_ERROR_GENERAL_EXCEPTION  = 1,
    FDB_ERROR_UNKNOWN_EXCEPTION  = 2,
    FDB_ITERATION_COMPLETE       = 3
};

const char* fdb_error_string(int err);

typedef void (*fdb_failure_handler_t)(void* context, int error_code);

/** Set a function to be called on error in addition to returning an error code.
 *  The handler can can access fdb_error_string()
 *  The handler can also access std::current_exception() if needed (and even rethrow if required).
 */
int fdb_set_failure_handler(fdb_failure_handler_t handler, void* context);

///@}


/** Types */

///@{

struct fdb_key_t;
typedef struct fdb_key_t fdb_key_t;

struct fdb_request_t;
typedef struct fdb_request_t fdb_request_t;

struct fdb_listiterator_t;
typedef struct fdb_listiterator_t fdb_listiterator_t;

struct fdb_datareader_t;
typedef struct fdb_datareader_t fdb_datareader_t;

struct fdb_handle_t;
typedef struct fdb_handle_t fdb_handle_t;

struct fdb_metadata_t;
typedef struct fdb_metadata_t fdb_metadata_t;

///@}

/** API */

///@{

/** Creates a fdb instance. */
int fdb_new_handle(fdb_handle_t** fdb);
/** disclaimer: this is a low-level API. The provided key and the corresponding data are not checked for consistency */
int fdb_archive(fdb_handle_t* fdb, fdb_key_t* key, const char* data, size_t length);
/** Archives multiple messages.
 * If mars request @req is not nullptr, number of messages and their metadata are checked against the provided request */
int fdb_archive_multiple(fdb_handle_t* fdb, fdb_request_t* req, const char* data, size_t length);
int fdb_list(fdb_handle_t* fdb, const fdb_request_t* req, bool duplicates, fdb_listiterator_t* it);
int fdb_retrieve(fdb_handle_t* fdb, fdb_request_t* req, fdb_datareader_t* dr);
int fdb_flush(fdb_handle_t* fdb);
/** Closes and destroys the fdb instance.
 *  Must be called for every fdb_t created.
 */
int fdb_delete_handle(fdb_handle_t* fdb);

///@}

/** Ancillary functions */

///@{

int fdb_new_key(fdb_key_t** key);
int fdb_key_add(fdb_key_t* key, const char* param, const char* value);
int fdb_delete_key(fdb_key_t* key);

int fdb_new_request(fdb_request_t** req);
int fdb_request_add(fdb_request_t* req, const char* param, const char* values[], int numValues);
int fdb_delete_request(fdb_request_t* req);

int fdb_new_listiterator(fdb_listiterator_t** it);
int fdb_listiterator_next(fdb_listiterator_t* it);
int fdb_listiterator_attrs(fdb_listiterator_t* it, const char** uri, size_t* off, size_t* len);
int fdb_listiterator_key_next(fdb_listiterator_t* it, bool* found, const char** key, const char** value);
int fdb_delete_listiterator(fdb_listiterator_t* it);

int fdb_new_datareader(fdb_datareader_t** dr);
int fdb_datareader_open(fdb_datareader_t* dr, long* size);
int fdb_datareader_close(fdb_datareader_t* dr);
int fdb_datareader_tell(fdb_datareader_t* dr, long* pos);
int fdb_datareader_seek(fdb_datareader_t* dr, long pos);
int fdb_datareader_skip(fdb_datareader_t* dr, long count);
int fdb_datareader_read(fdb_datareader_t* dr, void *buf, long count, long* read);
int fdb_delete_datareader(fdb_datareader_t* dr);

///@}

/*--------------------------------------------------------------------------------------------------------------------*/

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
// fdb5_api_fdb_H
