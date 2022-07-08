/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/* 
 * @file   dummy_daos.h
 * @author Nicolau Manubens
 * @date   Jun 2022
 */

#ifndef fdb5_dummy_daos_dummy_daos_dev_H
#define fdb5_dummy_daos_dummy_daos_dev_H

#include "daos.h"

#include "eckit/filesystem/PathName.h"

using eckit::PathName;

const PathName& dummy_daos_root();

const PathName& dummy_daos_get_handle_path(daos_handle_t handle);

#endif /* fdb5_dummy_daos_dummy_daos_dev_H */