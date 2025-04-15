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


/** \defgroup Initialisation */
/** @{ */

/** Initialises API, must be called before any other function
 * \note This is only required if being used from a context where **eckit::Main()** is not otherwise initialised.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_initialise();

/** @} */


/** \defgroup Version Accessors */
/** @{ */

/** Retrieves the release version of the library in human-readable format, e.g. ``5.10.8``
 * \param version Return variable for release version. Returned pointer valid throughout program lifetime.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_version(const char** version);
/** Retrieves version control checksum of the latest change, e.g. ``a88011c007a0db48a5d16e296934a197eac2050a``
 * \param version Return variable for version control checksum. Returned pointer valid throughout program lifetime.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_vcs_version(const char** sha1);

///@}


/** \defgroup Error Handling */
/** @{ */

/** Return codes */
enum FdbErrorValues {
    FDB_SUCCESS                 = 0,
    FDB_ERROR_GENERAL_EXCEPTION = 1,
    FDB_ERROR_UNKNOWN_EXCEPTION = 2,
    FDB_ITERATION_COMPLETE      = 3
};

/** Returns a human-readable error message for the last error given an error code
 * \param err Error code (#FdbErrorValues)
 * \returns Error message
 */
const char* fdb_error_string(int err);

/** Error handler callback function signature
 * \param context Error handler context
 * \param error_code Error code (#FdbErrorValues)
 */
typedef void (*fdb_failure_handler_t)(void* context, int error_code);

/** Sets an error handler which will be called on error with the supplied context and an error code
 * \param handler Error handler function
 * \param context Error handler context
 */
int fdb_set_failure_handler(fdb_failure_handler_t handler, void* context);

/** @} */


/** \defgroup Key */
/** @{ */

struct fdb_key_t;
/** Opaque type for the Key object. Holds the metadata of a Key. */
typedef struct fdb_key_t fdb_key_t;

/** Creates a Key instance.
 * \param key Key instance. Returned instance must be deleted using #fdb_delete_key.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_new_key(fdb_key_t** key);

/** Adds a metadata pair to a Key
 * \param key Key instance
 * \param param Metadata name
 * \param value Metadata value
 * \returns Return code (#FdbErrorValues)
 */
int fdb_key_add(fdb_key_t* key, const char* param, const char* value);

/** Deallocates Key object and associated resources.
 * \param key Key instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_key(fdb_key_t* key);

/** @} */


/** \defgroup Request */
/** @{ */

struct fdb_request_t;
/** Opaque type for the Request object. Holds the metadata of a Request. */
typedef struct fdb_request_t fdb_request_t;

/** Creates a Request instance.
 * \param req Request instance. Returned instance must be deleted using #fdb_delete_request.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_new_request(fdb_request_t** req);

/** Adds a metadata definition to a Request
 * \param req Request instance
 * \param param Metadata name
 * \param values Metadata values
 * \param numValues number of metadata values
 * \returns Return code (#FdbErrorValues)
 */
int fdb_request_add(fdb_request_t* req, const char* param, const char* values[], int numValues);

/** Get the Metadata values associated to a Request metadata
 * \param req Request instance
 * \param param Metadata name
 * \param values Metadata values
 * \param numValues number of metadata values
 * \returns Return code (#FdbErrorValues)
 */
int fdb_request_get(fdb_request_t* req, const char* param, char** values[], size_t* numValues);

/** Expand a Request
 * \param req Request instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_expand_request(fdb_request_t* req);

/** Deallocates Request object and associated resources.
 * \param req Request instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_request(fdb_request_t* req);

/** @} */


/** \defgroup SplitKey */
/** @{ */

struct fdb_split_key_t;
/** Opaque type for the SplitKey object. Holds the Keys associated with a ListElement. */
typedef struct fdb_split_key_t fdb_split_key_t;

/** Creates a SplitKey instance.
 * \param key SplitKey instance. Returned instance must be deleted using #fdb_delete_splitkey.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_new_splitkey(fdb_split_key_t** key);

/** Returns the next set of metadata in a SplitKey object. For a given ListElement, the SplitKey represents the Keys
 * associated with each level of the FDB index. Supports multiple fdb_split_key_t iterating over the same key.
 * \param it SplitKey instance
 * \param key Key metadata name
 * \param value Key metadata value
 * \param level level in the iondex of the current Key
 * \returns Return code (#FdbErrorValues)
 */
int fdb_splitkey_next_metadata(fdb_split_key_t* it, const char** key, const char** value, size_t* level);

/** Deallocates SplitKey object and associated resources.
 * \param key SplitKey instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_splitkey(fdb_split_key_t* key);

/** @} */


/** \defgroup ListIterator */
/** @{ */

struct fdb_listiterator_t;
/** Opaque type for the ListIterator object. Holds the fdb listing output. */
typedef struct fdb_listiterator_t fdb_listiterator_t;

/** Moves to the next ListElement in a ListIterator object.
 * \param it ListIterator instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_listiterator_next(fdb_listiterator_t* it);

/** Returns the attribute of the current ListElement in a ListIterator object.
 * \param it ListIterator instance
 * \param uri URI describing the resource (i.e. file path) storing the ListElement data (i.e. GRIB message)
 * \param off Offset within the resource referred by #uri
 * \param len Length in bytes of the ListElement data
 * \returns Return code (#FdbErrorValues)
 */
int fdb_listiterator_attrs(fdb_listiterator_t* it, const char** uri, size_t* off, size_t* len);

/** Lazy extraction of the key of a list element, key metadata can be retrieved with fdb_splitkey_next_metadata.
 * \param it SplitKey instance (must be already initialised by #fdb_new_splitkey)
 * \returns Return code (#FdbErrorValues)
 */
int fdb_listiterator_splitkey(fdb_listiterator_t* it, fdb_split_key_t* key);

/** Deallocates ListIterator object and associated resources.
 * \param it ListIterator instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_listiterator(fdb_listiterator_t* it);

/** @} */


/** \defgroup DataReader */
/** @{ */

struct fdb_datareader_t;
/** Opaque type for the DataReader object. Provides access to the binary data returned by a FDB retrieval. */
typedef struct fdb_datareader_t fdb_datareader_t;

/** Creates a DataReader instance.
 * \param dr DataReader instance. Returned instance must be deleted using #fdb_delete_datareader.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_new_datareader(fdb_datareader_t** dr);

/** Open a DataReader for data reading.
 * \param dr DataReader instance
 * \param size size of the opened DataReader
 * \returns Return code (#FdbErrorValues)
 */
int fdb_datareader_open(fdb_datareader_t* dr, long* size);

/** Close a DataReader.
 * \param dr DataReader instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_datareader_close(fdb_datareader_t* dr);

/** Returns the current read position in a DataReader.
 * \param dr DataReader instance
 * \param pos Read position (byte offset from the start of the DataReader)
 * \returns Return code (#FdbErrorValues)
 */
int fdb_datareader_tell(fdb_datareader_t* dr, long* pos);

/** Sets a new read position in a DataReader.
 * \param dr DataReader instance
 * \param pos New read position (byte offset from the start of the DataReader)
 * \returns Return code (#FdbErrorValues)
 */
int fdb_datareader_seek(fdb_datareader_t* dr, long pos);

/** Move forward the read position in a DataReader.
 * \param dr DataReader instance
 * \param count Offset w.r.t. the current read position retured by fdb_datareader_tell
 * \returns Return code (#FdbErrorValues)
 */
int fdb_datareader_skip(fdb_datareader_t* dr, long count);

/** Return size of internal datahandle in bytes.
 * \param dr DataReader instance
 * \param size Size of the DataReader
 * \returns Return code (#FdbErrorValues)
 */
int fdb_datareader_size(fdb_datareader_t* dr, long* size);

/** Read binary data from a DataReader to a given memory buffer.
 * \param dr DataReader instance
 * \param buf Pointer of the target memory buffer.
 * \param count Max size of the data to read
 * \param read Actual size of the data read from the DataReader into the memory buffer
 * \returns Return code (#FdbErrorValues)
 */
int fdb_datareader_read(fdb_datareader_t* dr, void* buf, long count, long* read);

/** Deallocates DataReader object and associated resources.
 * \param key DataReader instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_datareader(fdb_datareader_t* dr);

/** @} */


/** \defgroup FDB API */
/** @{ */

struct fdb_handle_t;
/** Opaque type for the FDB object. */
typedef struct fdb_handle_t fdb_handle_t;

/** Creates a FDB instance.
 * \param fdb FDB instance. Returned instance must be deleted using #fdb_delete_handle.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_new_handle(fdb_handle_t** fdb);

/** Creates a FDB instance from a YAML configuration.
 * \param fdb FDB instance. Returned instance must be deleted using #fdb_delete_handle.
 * \param system_config Override the system config with this YAML string
 * \param user_config Supply user level config with this YAML string
 * \returns Return code (#FdbErrorValues)
 */
int fdb_new_handle_from_yaml(fdb_handle_t** fdb, const char* system_config, const char* user_config);

/** Archives binary data to a FDB instance.
 * \warning this is a low-level API. The provided key and the corresponding data are not checked for consistency
 * \param fdb FDB instance.
 * \param key Key used for indexing and archiving the data
 * \param data Pointer to the binary data to archive
 * \param length Size of the data to archive with the given #key
 * \returns Return code (#FdbErrorValues)
 */
int fdb_archive(fdb_handle_t* fdb, fdb_key_t* key, const char* data, size_t length);

/** Archives multiple messages to a FDB instance.
 * \param fdb FDB instance.
 * \param req If Request #req is not nullptr, the number of messages and their metadata are checked against the provided
 * request
 * \param data Pointer to the binary data to archive. Metadata are extracted from data headers
 * \param length Size of the data to archive
 * \returns Return code (#FdbErrorValues)
 */
int fdb_archive_multiple(fdb_handle_t* fdb, fdb_request_t* req, const char* data, size_t length);

/** List all available data whose metadata matches a given user request.
 * \param fdb FDB instance.
 * \param req User Request
 * \param it ListIterator than can be used to retrieve metadata and attributes of all ListElement matching the user
 * Request #req
 * \param duplicates Boolean flag used to specify if duplicated ListElements are to be reported or not.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_list(fdb_handle_t* fdb, const fdb_request_t* req, fdb_listiterator_t** it, bool duplicates, int depth);

/** Return all available data whose metadata matches a given user request.
 * \param fdb FDB instance.
 * \param req User Request. Metadata of retrieved data must match with the user Request
 * \param dr DataReader than can be used to read extracted data
 * \returns Return code (#FdbErrorValues)
 */
int fdb_retrieve(fdb_handle_t* fdb, fdb_request_t* req, fdb_datareader_t* dr);

/** Force flushing of all write operations
 * \param key FDB instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_flush(fdb_handle_t* fdb);

/** Deallocates FDB object and associated resources.
 * \param key FDB instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_handle(fdb_handle_t* fdb);

/** @} */


/** \defgroup Wipe */
/** @{ */

struct fdb_wipe_element_t;
typedef struct fdb_wipe_element_t fdb_wipe_element_t;

struct fdb_wipe_iterator_t;
typedef struct fdb_wipe_iterator_t fdb_wipe_iterator_t;

/** Initiates a wipe operation on the FDB. This identifies data matching the given request and, optionally, deletes it.
 * \param fdb FDB instance.
 * \param req Request specifying which data should be considered for wiping.
 * \param doit If true, matching data will be deleted. If false, only a dry-run is performed.
 * \param porcelain If true, output is formatted for machine parsing.
 * \param unsafeWipeAll If true, unrecognised data will also be deleted.
 * \param[out] it Iterator instance used to step through affected elements. Must be deleted using
 * #fdb_delete_wipe_iterator.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_wipe(fdb_handle_t* fdb, fdb_request_t* req, bool doit, bool porcelain, bool unsafeWipeAll,
             fdb_wipe_iterator_t** it);

/** Moves to the next element in a wipe iterator.
 * \param it WipeIterator instance.
 * \param[out] element Pointer to the next #fdb_wipe_element_t. Must be deleted using #fdb_delete_wipe_element.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_wipe_iterator_next(fdb_wipe_iterator_t* it, fdb_wipe_element_t** element);

/** Deallocates WipeIterator object.
 * \param it WipeIterator instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_wipe_iterator(fdb_wipe_iterator_t* it);

/** Deallocates WipeElement object.
 * \param element WipeElement instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_wipe_element(fdb_wipe_element_t* element);

/** Returns a string representation of a WipeElement.
 * \param element WipeElement instance
 * \param str String describing the element. Pointer valid until next() call or object deletion.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_wipe_element_string(fdb_wipe_element_t* element, const char** str);


/** \defgroup Purge */
/** @{ */

struct fdb_purge_element_t;
typedef struct fdb_purge_element_t fdb_purge_element_t;

struct fdb_purge_iterator_t;
typedef struct fdb_purge_iterator_t fdb_purge_iterator_t;

/** Initiates a purge operation on the FDB. This identifies duplicate data matching the given request and, optionally,
 * deletes it.
 * \param fdb FDB instance.
 * \param req Request specifying which data should be considered for purging.
 * \param doit If true, matching data will be deleted. If false, only a dry-run is performed.
 * \param porcelain If true, output is formatted for machine parsing.
 * \param[out] it Iterator instance used to step through affected elements. Must be deleted using
 * #fdb_delete_purge_iterator.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_purge(fdb_handle_t* fdb, fdb_request_t* req, bool doit, bool porcelain, fdb_purge_iterator_t** it);

/** Moves to the next element in a purge iterator.
 * \param it PurgeIterator instance.
 * \param[out] element Pointer to the next #fdb_purge_element_t. Must be deleted using #fdb_delete_purge_element.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_purge_iterator_next(fdb_purge_iterator_t* it, fdb_purge_element_t** element);

/** Deallocates PurgeIterator object.
 * \param it PurgeIterator instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_purge_iterator(fdb_purge_iterator_t* it);

/** Deallocates PurgeElement object.
 * \param element PurgeElement instance
 * \returns Return code (#FdbErrorValues)
 */
int fdb_delete_purge_element(fdb_purge_element_t* element);

/** Returns a string representation of a PurgeElement.
 * \param element PurgeElement instance
 * \param str String describing the element. Pointer valid until next() call or object deletion.
 * \returns Return code (#FdbErrorValues)
 */
int fdb_purge_element_string(fdb_purge_element_t* element, const char** str);

/** @} */
/*--------------------------------------------------------------------------------------------------------------------*/

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
// fdb5_api_fdb_H
