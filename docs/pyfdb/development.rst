Development
############

Follow the guide in :ref:`installation-label`. We advise to use `uv` for installation of the build dependencies.

The version pinning for the python dependency of the `FDB` is by default disabled, which is what you want for
local development. To enable this pinning, on CI we set the environment variable `VERSION_SUFFIX`, which is
used to pin the correct `fdb5lib` wheel version.

To utilize your local fdb build, make sure `findlibs` is installed in your `venv` and export:

.. code-block:: sh

   export FINDLIBS_DISABLE_PACKAGE=yes

Furthermore set the `FDB5_HOME` environment variable to the build folder of the stack, to make the correct
version of the `FDB` visible for `findlibs`.

.. code-block:: sh

   export FDB5_HOME=<path_to_stack_folder>/build

Now you are able to install the `pyfdb` wheel from the build folder (or the `pyfdb-python-package-staging`) to your
`venv`. Switch to the build folder and execute, e.g.:

.. code-block:: sh

   uv pip install pyfdb-<version>-cp311-cp311-macosx_26_0_arm64.whl 
   # or
   uv pip install -e pyfdb-python-package-staging

You can check the installation afterwards by switching to the `pyfdb` tests folder in the `fdb` source folder and
execute `pytest` in there.

.. code-block:: sh

   cd <path_to_stack>/fdb/tests/pyfdb
   pytest

.. note

   Currently there is an issue with non-released file handles during a run of pytest, depending on
   the ulimits of your user this may lead to an error `OSError: [Errno 24] Too many open files:`.
   
   In that case re-run pytest with the `--lf` flag for executing only the last failed tests.
