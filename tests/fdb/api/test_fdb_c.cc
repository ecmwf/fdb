/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdlib>
#include <iostream>

#include "fdb5/api/fdb_c.h"
#include "fdb5/api/helpers/ListIterator.h"

using namespace fdb5;


int main(int argc, char **argv) {

    fdb_initialise_api();

    fdb_t** fdb = new fdb_t*;
    /** Creates a reader and opens the speficied file. */
    fdb_init(fdb);

    fdb_KeySet_t keys;
    keys.numKeys = 0;

    fdb_ToolRequest_t* toolreq;
    fdb_ToolRequest_init_all(&toolreq, &keys);

    fdb_ListElement_t** el = new fdb_ListElement_t*();
    fdb_ListElement_init(el);

    fdb_ListIterator_t* it;
    bool exist = true;

    fdb_list(*fdb, toolreq, &it);

    while (exist) {
        fdb_list_next(it, &exist, el);
        if (exist) {
            std::cout << *((ListElement*) *el) << std::endl;
        }
    }

    std::cout << "MarsRequest" << std::endl;
    fdb_ToolRequest_init_str(&toolreq, "expver=xxxx", &keys);
    fdb_list(*fdb, toolreq, &it);

    exist = true;
    while (exist) {
        fdb_list_next(it, &exist, el);
        if (exist) {
            std::cout << *((ListElement*) *el) << std::endl;
        }
    }

    fdb_ListElement_clean(el);
    fdb_list_clean(it);
    fdb_ToolRequest_clean(toolreq);

    fdb_clean(*fdb);

    return 0;
}

