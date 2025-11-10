from pyfdb import PyFDB, Config
from pyfdb.pyfdb_type import FDBToolRequest


def test_purge(empty_fdb_setup, build_grib_messages):
    fdb_config_path = empty_fdb_setup

    fdb_config = Config(fdb_config_path.read_text())
    pyfdb = PyFDB(fdb_config)

    initial_elements = list(pyfdb.list(FDBToolRequest(key_values={"class": "ea"})))

    assert len(initial_elements) == 0
    print(f"Elements initial: {len(initial_elements)}")

    pyfdb.archive(build_grib_messages.read_bytes())
    pyfdb.flush()

    archived_elements = list(pyfdb.list(FDBToolRequest(key_values={"class": "ea"})))
    assert len(archived_elements) == 96

    print(f"Elements after archive: {len(archived_elements)}")

    purge_iterator = pyfdb.purge(FDBToolRequest(key_values={"class": "ea"}), doit=True)
    purged_output = []

    for el in purge_iterator:
        print(el)
        purged_output.append(el)

    assert len(purged_output) > 0

    elements_after_first_purge = list(
        pyfdb.list(FDBToolRequest(key_values={"class": "ea"}))
    )

    print(f"Elements after purge: {len(elements_after_first_purge)}")
    assert len(archived_elements) == len(elements_after_first_purge)

    pyfdb.archive(build_grib_messages.read_bytes())
    pyfdb.flush()

    after_archive_elements = list(
        pyfdb.list(FDBToolRequest(key_values={"class": "ea"}))
    )

    print(
        f"Elements after masking archive: {len(after_archive_elements)}, before: {len(initial_elements)}"
    )
    assert len(after_archive_elements) > len(initial_elements)

    purge_iterator = pyfdb.purge(FDBToolRequest(key_values={"class": "ea"}), doit=True)
    purged_output = list(purge_iterator)

    assert len(purged_output) > 0

    elements_after_second_purge = list(
        pyfdb.list(FDBToolRequest(key_values={"class": "ea"}))
    )

    for el in elements_after_second_purge:
        print(el)

    print(f"Elements after second purge: {len(elements_after_second_purge)}")
    assert len(archived_elements) == len(elements_after_second_purge)
