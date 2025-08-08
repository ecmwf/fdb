# SPDX-License-Identifier: LGPL-3.0-or-later
import glob
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext

min_cpp_standard = 17
min_cmake_version = "3.19"
version = "0.0.1"

def check_cmake():
    try:
        result = subprocess.run(
            ["cmake", "--version"], check=True, capture_output=True, text=True
        )
        # Check CMake version
        found_cmake_version = re.search(
            r"(\d+).(\d+).(\d+)", str(result.stdout)
        )
        for min_version, found_version in zip(
            min_cmake_version.split("."), found_cmake_version.groups()
        ):
            if found_version > min_version:
                return True
            if found_version < min_version:
                return False
    except Exception as _:
        return False
    return True


def check_cpp_compiler():
    with tempfile.TemporaryDirectory() as tmp_dir:
        simple_main = textwrap.dedent(
            """
            int main(){ 
                return 0;
            }
            """
        )
        cpp_file = tmp_dir + "/main.cpp"
        with open(cpp_file, "w") as cp_file:
            cp_file.write(simple_main)

        simple_cmake = textwrap.dedent(
            f"""
            cmake_minimum_required(VERSION 3.19)
            project(SimpleTest)
            set(CMAKE_CXX_STANDARD {min_cpp_standard})
            set(CMAKE_CXX_STANDARD_REQUIRED ON)
            add_executable(simple_test main.cpp)
            """
        )
        cmake_file = tmp_dir + "/CMakeLists.txt"
        with open(cmake_file, "w") as cm_file:
            cm_file.write(simple_cmake)

        tmp_dir_build = pathlib.Path(tmp_dir) / "build"
        tmp_dir_build.mkdir()

        try:
            subprocess.run(
                ["cmake", "-S", str(tmp_dir), "-B", str(tmp_dir_build)],
                check=True,
            )
        except Exception as _:
            return False
    return True


# A CMakeExtension needs a sourcedir instead of a file list.
# The name must be the _single_ output extension from the CMake build.
# If you need multiple extensions, see scikit-build.
class CMakeExtension(Extension):
    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = os.fspath(Path(sourcedir).resolve())


class CMakeBuild(build_ext):
    def build_extension(self, ext: CMakeExtension) -> None:
        if not check_cmake():
            raise ModuleNotFoundError(
                f"No CMake or no CMake >= {min_cmake_version} installation "
                f"found on the system, please install "
                f"CMake >= {min_cmake_version}."
            )

        if not check_cpp_compiler():
            raise ModuleNotFoundError(
                "Could not compile a simple C++ program, "
                f"please install a C++ compiler with C++{min_cpp_standard} "
                f"support."
            )

        # Must be in this form due to bug in .resolve() only fixed in Python 3.10+
        ext_fullpath = Path.cwd() / self.get_ext_fullpath(ext.name)
        extdir = ext_fullpath.parent.resolve()

        # Using this requires trailing slash for auto-detection & inclusion of
        # auxiliary "native" libs
        debug = (
            int(os.environ.get("DEBUG", 0))
            if self.debug is None
            else self.debug
        )
        cfg = "Debug" if debug else "Release"

        # CMake lets you override the generator - we need to check this.
        # Can be set with Conda-Build, for example.
        cmake_generator = os.environ.get("CMAKE_GENERATOR", "")

        # Set Python_EXECUTABLE instead if you use PYBIND11_FINDPYTHON
        # EXAMPLE_VERSION_INFO shows you how to pass a value into the C++ code
        # from Python.
        cmake_args = [
            f"-DCMAKE_BUILD_TYPE={cfg}",  # not used on MSVC, but no harm
            f"-DPython_EXECUTABLE={sys.executable}",
        ]

        # Pile all .so in one place and use $ORIGIN as RPATH
        cmake_args += ["-DCMAKE_BUILD_WITH_INSTALL_RPATH=TRUE"]
        #cmake_args += ["-DCMAKE_INSTALL_RPATH={}".format("$ORIGIN")]

        build_args = ["-j"]
        # Adding CMake arguments set as environment variable
        # (needed e.g. to build for ARM OSx on conda-forge)
        if "CMAKE_ARGS" in os.environ:
            cmake_args += [
                item for item in os.environ["CMAKE_ARGS"].split(" ") if item
            ]

        if not cmake_generator or cmake_generator == "Ninja":
            try:
                import ninja

                ninja_executable_path = Path(ninja.BIN_DIR) / "ninja"
                cmake_args += [
                    "-GNinja",
                    f"-DCMAKE_MAKE_PROGRAM:FILEPATH={ninja_executable_path}",
                ]
            except ImportError:
                pass


        if sys.platform.startswith("darwin"):
            # Cross-compile support for macOS - respect ARCHFLAGS if set
            archs = re.findall(r"-arch (\S+)", os.environ.get("ARCHFLAGS", ""))
            if archs:
                cmake_args += [
                    "-DCMAKE_OSX_ARCHITECTURES={}".format(";".join(archs))
                ]

        build_temp = Path(self.build_temp) / ext.name
        if not build_temp.exists():
            build_temp.mkdir(parents=True)


        subprocess.run(
            ["cmake", ext.sourcedir, *cmake_args], cwd=build_temp, check=True
        )
        subprocess.run(
            ["cmake", "--build", ".", *build_args], cwd=build_temp, check=True
        )

        # Copy library files to root build folder
        files = glob.glob(str(build_temp) + "/lib/chunked_data_view_bindings*.so")
        files += glob.glob(str(build_temp) + "/lib/libchunked_data_view*.so")
        files += glob.glob(str(build_temp) + "/lib/libchunked_data_view*.dylib")
        destination = extdir / "pychunked_data_view"

        for lib_file in files:
            f = pathlib.Path(lib_file)
            shutil.copy(dst=destination, src=f)


# The information here can also be placed in setup.cfg - better separation of
# logic and declaration, and simpler if you include description/version in a file.
setup(
    name="z3fdb",
    version=version,
    description="zarrv3fdb",
    ext_modules=[CMakeExtension("chunked_data_view_bindings", sourcedir="../../..")],
    cmdclass={"build_ext": CMakeBuild},
    zip_safe=False,
    python_requires=">=3.10,<3.14",
    packages=[
        "z3fdb",
        "pychunked_data_view",
    ],
    package_dir={
        "z3fdb": "../../src/z3fdb",
        "pychunked_data_view": "../../src/pychunked_data_view",
    },
    install_requires=[
        "fdb5lib==5.17.3",
        "zarr>=3.0",
        "numpy",
        "findlibs"
    ],
)
