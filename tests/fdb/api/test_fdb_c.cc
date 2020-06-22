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

    char str[300];
    fdb_listiterator_t* it;
    fdb_listiterator_init(&it);
    bool exist = true;

    fdb_list(*fdb, nullptr, it);

    while (exist) {
        fdb_listiterator_next(it, &exist, str, 300);
        if (exist) {
            std::cout << str << std::endl;
        }
    }

    std::cout << "MarsRequest" << std::endl;
    fdb_request_t* toolreq;
    fdb_request_init(&toolreq);
    char* val = "xxxx";
    char* values[] = {val};

    fdb_request_add(toolreq, "expver", values, 1);
    fdb_list(*fdb, toolreq, it);

    exist = true;
    while (exist) {
        fdb_listiterator_next(it, &exist, str, 300);
        if (exist) {
            std::cout << str << std::endl;
        }
    }

    fdb_listiterator_clean(it);
    fdb_request_clean(toolreq);

    fdb_clean(*fdb);

    return 0;
}

