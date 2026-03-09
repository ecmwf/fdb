#!/usr/bin/env bash
# Ensure that the database corresponding to a request does not exist

ver=$(fdb-info --version)

[[ ! "$ver" =~ ^5.[0-9]+.[0-9]+$ ]] && echo "expected version number" && exit 1

echo $(fdb-info --version)

if ! (fdb-info --all | grep -q "Version: $ver")
then
    echo "Expected version in --all output"
    exit 1
fi


