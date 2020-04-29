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

#include "fdb5/api/fdc.h"
#include "fdb5/api/helpers/ListIterator.h"

using namespace fdb5;


int main(int argc, char **argv) {

    fdc_initialise_api();

    fdc_t** fdb = new fdc_t*;
    /** Creates a reader and opens the speficied file. */
    fdc_init(fdb);

    fdc_KeySet_t keys;
    keys.numKeys = 0;

    fdc_ToolRequest_t* toolreq;
    fdc_ToolRequest_init_all(&toolreq, &keys);

    fdc_ListElement_t* el;
    fdc_ListElement_init(&el);

    fdc_ListIterator_t* it;
    bool exist = true;

    fdc_list(&it, *fdb, toolreq);

    while (exist) {
        fdc_list_next(it, &exist, &el);
        if (exist) {
            std::cout << *((ListElement*) el) << std::endl;
        }
    }

    std::cout << "MarsRequest" << std::endl;
    fdc_ToolRequest_init_str(&toolreq, "expver=xxxx", &keys);
    fdc_list(&it, *fdb, toolreq);

    exist = true;
    while (exist) {
        fdc_list_next(it, &exist, &el);
        if (exist) {
            std::cout << *((ListElement*) el) << std::endl;
        }
    }

    fdc_ListElement_clean(&el);
    fdc_list_clean(it);
    fdc_ToolRequest_clean(toolreq);

    fdc_clean(*fdb);

    return 0;
}

