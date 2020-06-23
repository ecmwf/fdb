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

    fdb_handle_t** fdb = new fdb_handle_t*;
    /** Creates a reader and opens the speficied file. */
    fdb_new_handle(fdb);

    const char** ptr = new const char*;
    fdb_listiterator_t* it;
    fdb_new_listiterator(&it);
    bool exist = true;

    fdb_list(*fdb, nullptr, it);

    while (exist) {
        fdb_listiterator_next(it, &exist, ptr);
        if (exist) {
            std::cout << *ptr << std::endl;
        }
    }

    std::cout << "MarsRequest" << std::endl;
    fdb_request_t* toolreq;
    fdb_new_request(&toolreq);
    char* val = "xxxx";
    char* values[] = {val};

    fdb_request_add(toolreq, "expver", values, 1);
    fdb_list(*fdb, toolreq, it);

    exist = true;
    while (exist) {
        fdb_listiterator_next(it, &exist, ptr);
        if (exist) {
            std::cout << *ptr << std::endl;
        }
    }

    fdb_delete_listiterator(it);
    fdb_delete_request(toolreq);

    fdb_delete_handle(*fdb);

    return 0;
}

