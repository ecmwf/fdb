#pragma once
#include "data_map.h"
#include <unordered_map>
#include <fdb5/api/FDB.h>
#include <fdb5/api/helpers/FDBToolRequest.h>

namespace compare::common {

void assemble_compare_map(fdb5::FDB& fdb,
                        DataIndex& out,
                        const fdb5::FDBToolRequest& req,
                        const KeyMap& ignore);

}