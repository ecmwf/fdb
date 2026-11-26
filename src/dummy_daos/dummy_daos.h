// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * @file   dummy_daos.h
 * @author Nicolau Manubens
 * @date   Jun 2022
 */

#pragma once

#include "daos.h"

#include "eckit/filesystem/PathName.h"

using eckit::PathName;

//----------------------------------------------------------------------------------------------------------------------

const PathName& dummy_daos_root();

const PathName& dummy_daos_get_handle_path(daos_handle_t handle);

//----------------------------------------------------------------------------------------------------------------------