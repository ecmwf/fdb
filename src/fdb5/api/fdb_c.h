/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/** @author Simon Smart */
/** @author Emanuele Danovaro */
/** @date   Apr 2020 */

#ifndef fdb5_api_fdb_H
#define fdb5_api_fdb_H

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

int fdb_initialise_api();

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

enum FdcErrorValues {
    FDB_SUCCESS                  = 0,
    FDB_ERROR_GENERAL_EXCEPTION  = 1,
    FDB_ERROR_UNKNOWN_EXCEPTION  = 2
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

struct fdb_t;
typedef struct fdb_t fdb_t;

///@}

/** API */

///@{

/** Creates a fdb instance. */
int fdb_init(fdb_t** fdb);
int fdb_archive(fdb_t* fdb, fdb_key_t* key, const char* data, size_t length);
int fdb_list(fdb_t* fdb, const fdb_request_t* req, fdb_listiterator_t* it);
int fdb_retrieve(fdb_t* fdb, fdb_request_t* req, fdb_datareader_t* dr);
/** Closes and destroys the fdb instance.
 *  Must be called for every fdb_t created.
 */
int fdb_clean(fdb_t* fdb);

///@}

/** Ancillary functions */

///@{

int fdb_key_init(fdb_key_t** key);
int fdb_key_add(fdb_key_t* key, char* param, char* value);
int fdb_key_clean(fdb_key_t* key);

int fdb_request_init(fdb_request_t** req);
int fdb_request_add(fdb_request_t* req, char* param, char* values[], int numValues);
int fdb_request_clean(fdb_request_t* req);

int fdb_listiterator_init(fdb_listiterator_t** it);
int fdb_listiterator_next(fdb_listiterator_t* it, bool* exist, char* str, size_t length);
int fdb_listiterator_clean(fdb_listiterator_t* it);

int fdb_datareader_init(fdb_datareader_t** dr);
int fdb_datareader_open(fdb_datareader_t* dr);
int fdb_datareader_close(fdb_datareader_t* dr);
int fdb_datareader_tell(fdb_datareader_t* dr, long* pos);
int fdb_datareader_seek(fdb_datareader_t* dr, long pos);
int fdb_datareader_skip(fdb_datareader_t* dr, long count);
int fdb_datareader_read(fdb_datareader_t* dr, void *buf, long count, long* read);
int fdb_datareader_clean(fdb_datareader_t* dr);

///@}

/*--------------------------------------------------------------------------------------------------------------------*/

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
// fdb5_api_fdb_H
