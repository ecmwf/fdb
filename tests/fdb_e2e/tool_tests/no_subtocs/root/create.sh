#!/usr/bin/env bash
set -euxo pipefail

# Ensure that the database corresponding to a request does not exist

SIMPLE_REQUEST=$(cat req.xxxy.simple)

fdb-where --ignore-errors=false $SIMPLE_REQUEST && echo "Database already exists" && exit-1

# By default, we don't create a database just by asking where it is

fdb-root $SIMPLE_REQUEST
fdb-where --ignore-errors=false $SIMPLE_REQUEST && echo "Database incorrectly created" && exit-1

# We can create a database if needed!

fdb-root --create $SIMPLE_REQUEST
fdb-where --ignore-errors=false $SIMPLE_REQUEST

exit 0
