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


/** Types and ancillary functions */

///@{

struct fdb_Key_t;
typedef struct fdb_Key_t fdb_Key_t;

struct fdb_KeySet_t {
    int numKeys;
    char *keySet[];
};
typedef struct fdb_KeySet_t fdb_KeySet_t;

struct fdb_MarsRequest_t;
typedef struct fdb_MarsRequest_t fdb_MarsRequest_t;

struct fdb_ToolRequest_t;
typedef struct fdb_ToolRequest_t fdb_ToolRequest_t;

struct fdb_ListElement_t;
typedef struct fdb_ListElement_t fdb_ListElement_t;

struct fdb_ListIterator_t;
typedef struct fdb_ListIterator_t fdb_ListIterator_t;

struct fdb_DataReader_t;
typedef struct fdb_DataReader_t fdb_DataReader_t;

struct fdb_t;
typedef struct fdb_t fdb_t;

int fdb_Key_init(fdb_Key_t** key);
int fdb_Key_set(fdb_Key_t* key, char* k, char* v);
int fdb_Key_clean(fdb_Key_t* key);

int fdb_KeySet_clean(fdb_KeySet_t* keySet);

int fdb_MarsRequest_init(fdb_MarsRequest_t** req, char* str);
int fdb_MarsRequest_value(fdb_MarsRequest_t* req, char* name, char* values[], int numValues);
int fdb_MarsRequest_parse(fdb_MarsRequest_t** req, char* str);
int fdb_MarsRequest_clean(fdb_MarsRequest_t* req);

int fdb_ToolRequest_init_all(fdb_ToolRequest_t** req, fdb_KeySet_t *keys);
int fdb_ToolRequest_init_mars(fdb_ToolRequest_t** req, fdb_MarsRequest_t *marsReq, fdb_KeySet_t *keys);
int fdb_ToolRequest_init_str(fdb_ToolRequest_t** req, char *str, fdb_KeySet_t *keys);
int fdb_ToolRequest_clean(fdb_ToolRequest_t* req);

int fdb_ListElement_init(fdb_ListElement_t** el);
int fdb_ListElement_str(fdb_ListElement_t* el, char **str);
int fdb_ListElement_clean(fdb_ListElement_t** el);

int fdb_DataReader_open(fdb_DataReader_t* dr);
int fdb_DataReader_close(fdb_DataReader_t* dr);
int fdb_DataReader_tell(fdb_DataReader_t* dr, long* pos);
int fdb_DataReader_seek(fdb_DataReader_t* dr, long pos);
int fdb_DataReader_skip(fdb_DataReader_t* dr, long count);
int fdb_DataReader_read(fdb_DataReader_t* dr, void *buf, long count, long* read);
int fdb_DataReader_clean(fdb_DataReader_t* dr);

///@}


/** API */

///@{

/** Creates a fdb instance. */
int fdb_init(fdb_t** fdb);

/** Closes and destroys the fdb instance.
 *  Must be called for every fdb_t created.
 */
int fdb_clean(fdb_t* fdb);

// -------------- Primary API functions ----------------------------

int fdb_list(fdb_t* fdb, const fdb_ToolRequest_t* req, fdb_ListIterator_t** it);
int fdb_list_next(fdb_ListIterator_t* it, bool* exist, fdb_ListElement_t** el);
int fdb_list_clean(fdb_ListIterator_t* it);

int fdb_archive(fdb_t* fdb, fdb_Key_t* key, const void* data, size_t length);

typedef long (*fdb_stream_write_t)(void* context, const void* data, long length);

int fdb_retrieve(fdb_t* fdb, fdb_MarsRequest_t* req, fdb_DataReader_t** dr);

/// Flushes all buffers and closes all data handles into a consistent DB state
/// @note always safe to call
void flush();

///@}

/*--------------------------------------------------------------------------------------------------------------------*/

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
// fdb5_api_fdb_H
