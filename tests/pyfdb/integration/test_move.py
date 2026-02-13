from pathlib import Path
from typing import List

import yaml

from pyfdb.pyfdb import URI, FDB
from pyfdb.pyfdb_iterator import MoveElement


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

    print(
        "------------------------------COLLECTING FILES FORM OLD DB DIRECTORY------------------------------"
    )

    listed_elements = list(fdb.list(selection, level=3))
    parent_directories = list(set(Path(str(el.uri())).parent for el in listed_elements))

    # in the read only fdb setup all files share the same directory
    assert len(parent_directories) == 1
    parent_directory = parent_directories[0]

    files_before_move = list(parent_directory.iterdir())
    print("\n".join((str(x) for x in files_before_move)))
    assert len(files_before_move) == 4

    print(
        f"------------------------------MOVING FDB TO NEW ROOT: {str(new_root)}------------------------------"
    )

    move_iterator = fdb.move(
        selection,
        URI(new_root),
    )

    elements: List[MoveElement] = []

    for el in move_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 8  # 7 elements plus the directory

    # Only iterate of all but the last element.
    # The last element is used from the fdb-move tool to trigger the deletion of the src directory
    # This is not what we want in this regard.
    for el in elements[:-1]:
        el.execute()

    print("------------------------------NEW ROOT CONTENT:------------------------------")
    database_dirs = list(new_root.iterdir())
    assert len(database_dirs) == 1
    print(database_dirs[0])
    files_after_move = list(database_dirs[0].iterdir())
    print("\n".join([str(el) for el in files_after_move]))
    assert len(files_after_move) == 7

    print("------------------------------COMPARISON:------------------------------")
    # Check that all file from before the move are part of the directory after the move
    for f in files_before_move:
        print(f.name)
        assert f.name in [f.name for f in files_after_move]

    assert "wipe.lock" in [f.name for f in files_after_move]
    assert "archive.lock" in [f.name for f in files_after_move]
    assert "duplicates.allow" in [f.name for f in files_after_move]

    print("------------------------------CHECK ORIGIN DIRECTORY:------------------------------")
    # Check that the files are still in the original directory
    assert len(parent_directories) == 1
    parent_directory = parent_directories[0]

    assert len(list(parent_directory.iterdir())) != 0

    files_before_move = list(parent_directory.iterdir())
    print("\n".join((str(x) for x in files_before_move)))
    assert len(files_before_move) == 7  # The lock files have been added


def test_move_cleanup(read_only_fdb_setup):
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

    print(
        "------------------------------COLLECTING FILES FORM OLD DB DIRECTORY------------------------------"
    )

    listed_elements = list(fdb.list(selection, level=3))
    # Collect the directory of all listed elements
    parent_directories = list(set(Path(str(el.uri())).parent for el in listed_elements))

    # in the read only fdb setup all files share the same directory
    assert len(parent_directories) == 1
    parent_directory = parent_directories[0]

    files_before_move = list(parent_directory.iterdir())
    print("\n".join((str(x) for x in files_before_move)))
    assert len(files_before_move) == 4

    print(
        f"------------------------------MOVING FDB TO NEW ROOT: {str(new_root)}------------------------------"
    )

    move_iterator = fdb.move(
        selection,
        URI(new_root),
    )

    elements: List[MoveElement] = []

    for el in move_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 8  # 7 elements plus the directory

    # Only iterate of all but the last element.
    # The last element is used from the fdb-move tool to trigger the deletion of the src directory
    # This is not what we want in this regard.
    for el in elements[:-1]:
        el.execute()
        el.cleanup()

    print("------------------------------NEW ROOT CONTENT:------------------------------")
    database_dirs = list(new_root.iterdir())
    assert len(database_dirs) == 1
    print(database_dirs[0])
    files_after_move = list(database_dirs[0].iterdir())
    print("\n".join([str(el) for el in files_after_move]))
    assert len(files_after_move) == 7

    print("------------------------------COMPARISON:------------------------------")
    # Check that all file from before the move are part of the directory after the move
    for f in files_before_move:
        print(f.name)
        assert f.name in [f.name for f in files_after_move]

    assert "wipe.lock" in [f.name for f in files_after_move]
    assert "archive.lock" in [f.name for f in files_after_move]
    assert "duplicates.allow" in [f.name for f in files_after_move]

    print("------------------------------CHECK ORIGIN DIRECTORY:------------------------------")
    # Check that the files are still in the original directory
    assert len(parent_directories) == 1
    parent_directory = parent_directories[0]

    assert len(list(parent_directory.iterdir())) == 0
