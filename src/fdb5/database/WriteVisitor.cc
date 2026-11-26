// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/WriteVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

WriteVisitor::WriteVisitor(std::vector<Key>& prev) : prev_(prev) {
    prev.resize(3);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
