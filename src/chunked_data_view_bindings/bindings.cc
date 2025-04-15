/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */
#include <pybind11/pybind11.h>

#include <chunked_data_view/chunked_data_view.h>

int add(int i, int j) {
    return i + j;
}

PYBIND11_MODULE(pychunked_data_view, m) {
    m.doc() = "pybind11 example plugin";  // optional module docstring

    m.def("add", &add, "A function that adds two numbers");
}
