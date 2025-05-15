#! /usr/bin/env python

import requests
import json
import os
import re


def main():
    prefix = "https://sites.ecmwf.int/docs/dev-section"
    method = "s/api/v2/files"
    API_URL = f"{prefix}/{method}/fdb"
    headers = {
        "accept": "*/*",
        "Authorization": f"Bearer {os.environ['ECMWF_SITES_TOKEN']}",
    }
    resp = requests.get(
        API_URL,
        headers=headers,
        timeout=30,
        params={"list": "true", "limit": "64000", "type": "d"},
    )
    resp.raise_for_status()
    data: dict = resp.json()

    regex = re.compile(r"^(?:master|develop|\d+\.\d+\.\d+)$")
    versions = [
        {"name": x["path"], "version": x["path"], "url": f"{prefix}/fdb/{x['path']}"}
        for x in data["files"]
        if regex.match(x["path"])
    ]
    res = requests.put(
        f"{API_URL}/versions.json",
        files={"file": json.dumps(versions).encode("utf-8")},
        headers=headers,
        timeout=30,
    )
    print(res.status_code)


if __name__ == "__main__":
    main()
