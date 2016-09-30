/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sep 2016


#ifndef fdb5_pmem_PMemDataPoolManager_H
#define fdb5_pmem_PMemDataPoolManager_H


namespace fdb5 {

// -------------------------------------------------------------------------------------------------

/*
 * This class needs to:
 *
 * - Open data pools for reading when they are required. Given all the memory is there. Maintain
 *   the mapping with uuids.
 * - When a pool is filled up (writing), open a new one
 * - Generate the pool filenames based off the filename of the DB pool, + a counting integer
 * - Manage atomic allocations, with retries.
 */


// -------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_pmem_PMemDataPoolManager_H
