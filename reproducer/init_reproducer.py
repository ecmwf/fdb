#! /usr/bin/env python 

from pathlib import Path
import yaml

catalogue_config = {
    "type": "catalogue",
    "serverPort": 8000,
    "engine": "toc",
    "schema": "???",
    "supportedConnections": [1],
    "spaces": [{"handler": "Default", "roots": [{"path": "???"}]}],
    "stores": [{"default": "localhost:8001", "serveLocalData": True}],
}

store_config = {
    "type": "store",
    "serverPort": 8001,
    "store": "file",
    "spaces": [{"handler": "Default", "roots": [{"path": "???"}]}],
}

remote_config = {
    "type": "remote",
    "host": "localhost",
    "port": 8000,
    "engine": "remote",
    "store": "remote",
}


def main():
    location = Path(__file__).parent
    print("Initializing reproducer @ {location}")
    db_store = location / "db_store"
    schema = location / "schema"
    catalogue_config_file = location / "catalogue_config_file.yaml"
    store_config_file = location / "store_config_file.yaml"
    remote_config_file = location / "remote_config_file.yaml"

    catalogue_config["schema"] = str(schema)
    catalogue_config["spaces"][0]["roots"][0]["path"] = str(db_store)

    store_config["spaces"][0]["roots"][0]["path"] = str(db_store)

    print("Creating config files:")
    catalogue_config_str = yaml.dump(
        catalogue_config, sort_keys=False, default_flow_style=False
    )
    print("=== Catalogue Config ===")
    print(catalogue_config_str)
    catalogue_config_file.write_text(catalogue_config_str)

    store_config_str = yaml.dump(
        store_config, sort_keys=False, default_flow_style=False
    )
    print("=== Store Config ===")
    print(store_config_str)
    store_config_file.write_text(store_config_str)

    remote_config_str = yaml.dump(
        remote_config, sort_keys=False, default_flow_style=False
    )
    print("=== Remote Config ===")
    print(remote_config_str)
    remote_config_file.write_text(remote_config_str)

if __name__ == "__main__":
    main()
