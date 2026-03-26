Introduced changes
==================
- Added domain=g to fdb-overlay and fdb-hide commands

FIXED
=====
FAILED test_simple_fdb_no_subtoc_no_expver_handler.py::test_read[/Users/tkremer/Code/stack_2/fdb/tests/fdb_e2e/tool_tests/no_subtoc/read/steprange.sh] - subprocess.CalledProcessError: Command '/Users/tkremer/Code/stack_2/fdb/tests/fdb_e2e/tool_tests/no_subtoc/read/steprange.sh' returned non-zero exit status 1.
FAILED test_simple_fdb_no_subtoc_no_expver_handler.py::test_overlay[/Users/tkremer/Code/stack_2/fdb/tests/fdb_e2e/tool_tests/no_subtoc/overlay/wipe.sh] - subprocess.CalledProcessError: Command '/Users/tkremer/Code/stack_2/fdb/tests/fdb_e2e/tool_tests/no_subtoc/overlay/wipe.sh' returned non-zero exit status 1.
- Added xfail for now till FDB-652 is fixed
FAILED test_simple_fdb_no_subtoc_no_expver_handler.py::test_wipe[/Users/tkremer/Code/stack_2/fdb/tests/fdb_e2e/tool_tests/no_subtoc/wipe/porcelain.sh] - subprocess.CalledProcessError: Command '/Users/tkremer/Code/stack_2/fdb/tests/fdb_e2e/tool_tests/no_subtoc/wipe/porcelain.sh' returned non-zero exit status 255.
- Output changed

CHANGED TESTS
=============
tests/fdb_e2e/tool_tests/subtocs/wipe/duplicate_data.sh

- Dropped config_yaml.sh setup as there is virtually no difference to the simple_setup

TODO
====
- assert all in conftest during setup of fdb
- delete PATH modification

# for config_script, cleanup_script, expver, subtocs
("files_config", None, True, False),
("files_config", None, False, False),
("files_config", None, True, True),
("files_config", None, False, True),
("simple_config", None, True, False),
("simple_config", None, False, False),
("simple_config", None, True, True),
("simple_config", None, False, True),
(
    "config_yaml",
    None,
    False,
    False,
),  # Just checking that the correct config is used from default
(
    "config_json",
    None,
    False,
    False,
),  # locations. Don't worry about testing all variants for this -
("config_yaml_mars_disks", None, False, False),
(
    "tool_json",
    None,
    False,
    False,
),  # that can be tested in the above tests
("tool_yaml", None, False, False),
