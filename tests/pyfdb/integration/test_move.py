from pathlib import Path

import yaml

from pyfdb.pyfdb import URI, FDB


def add_new_root(fdb_config_path: Path, new_root: Path) -> str:
    # Manipulate the config of the FDB to contain a new root
    fdb_config = yaml.safe_load(fdb_config_path.read_text())

    fdb_config["spaces"][0]["roots"].append({"path": str(new_root)})

    new_root.mkdir()

    fdb_config_string = yaml.dump(fdb_config)
    fdb_config_path.write_text(fdb_config_string)

    return fdb_config_string


def test_move(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    new_root: Path = fdb_config_path.parent / "new_db"
    updated_config = add_new_root(fdb_config_path, new_root)

    print(updated_config)

    fdb = FDB(updated_config)

    # Selection for the second level of the schema
    selection = {
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "time": "1800",
    }

    print(str(new_root))

    move_iterator = fdb.move(
        selection,
        URI.from_str(str(new_root)),
    )

    elements = []

    for el in move_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 8

    # Only iterate of all but the last element.
    # The last element is used from the fdb-move tool to trigger the deletion of the src folder
    # This is not what we want in this regard.
    for el in elements[:-1]:
        el.execute()

    assert len(list(new_root.iterdir())) == 1
