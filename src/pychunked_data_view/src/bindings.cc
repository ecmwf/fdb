#include <pybind11/pybind11.h>

#include <chunked_data_view/chunked_data_view.h>

int add(int i, int j) {
    return i + j;
}

PYBIND11_MODULE(pychunked_data_view, m) {
    m.doc() = "pybind11 example plugin"; // optional module docstring

    m.def("add", &add, "A function that adds two numbers");
}
