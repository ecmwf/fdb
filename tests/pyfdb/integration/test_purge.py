# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from pyfdb import FDB


def test_purge_dryrun_no_purge(empty_fdb_setup, build_grib_messages):
    fdb = FDB(empty_fdb_setup)

    initial_elements = list(fdb.list({"class": "ea"}))

    assert len(initial_elements) == 0
    print(f"Elements initial: {len(initial_elements)}")

    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    print("Archiving again")
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    after_archive_elements = list(fdb.list({"class": "ea"}))

    print(
        f"Elements after masking archive: {len(after_archive_elements)}, before: {len(initial_elements)}"
    )
    assert len(after_archive_elements) > len(initial_elements)

    purge_iterator = fdb.purge({"class": "ea"})
    purged_output = list(purge_iterator)

    assert len(purged_output) > 0

    elements_after_second_purge = list(fdb.list({"class": "ea"}))

    for el in elements_after_second_purge:
        print(el)

    print(f"Elements after second purge: {len(elements_after_second_purge)}")
    assert len(after_archive_elements) == len(elements_after_second_purge)


def test_purge(empty_fdb_setup, build_grib_messages):
    fdb = FDB(empty_fdb_setup)

    initial_elements = list(fdb.list({"class": "ea"}))

    assert len(initial_elements) == 0
    print(f"Elements initial: {len(initial_elements)}")

    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    archived_elements = list(fdb.list({"class": "ea"}))
    assert len(archived_elements) == 96

    print(f"Elements after archive: {len(archived_elements)}")

    purge_iterator = fdb.purge({"class": "ea"}, doit=True)
    purged_output = []

    for el in purge_iterator:
        print(el)
        purged_output.append(el)

    assert len(purged_output) > 0

    elements_after_first_purge = list(fdb.list({"class": "ea"}))

    print(f"Elements after purge: {len(elements_after_first_purge)}")
    assert len(archived_elements) == len(elements_after_first_purge)

    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    after_archive_elements = list(fdb.list({"class": "ea"}))

    print(
        f"Elements after masking archive: {len(after_archive_elements)}, before: {len(initial_elements)}"
    )
    assert len(after_archive_elements) > len(initial_elements)

    purge_iterator = fdb.purge({"class": "ea"}, doit=True)
    purged_output = list(purge_iterator)

    assert len(purged_output) > 0

    elements_after_second_purge = list(fdb.list({"class": "ea"}))

    for el in elements_after_second_purge:
        print(el)

    print(f"Elements after second purge: {len(elements_after_second_purge)}")
    assert len(archived_elements) == len(elements_after_second_purge)
