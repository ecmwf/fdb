#!/usr/bin/env bash

# Runs the cross-client case of fdb_test_database_nfs from several independent NFS clients.
#
# Mounts of one export made from the same network namespace share a single NFS client: one cache
# and one lock owner, so they cannot show what the TOC locks are for. Here every client gets its
# own network namespace, hostname (the NFSv4 client identity) and mount, like a separate machine.
# The clients append to one main TOC at the same time; a last client then checks that no record
# was lost.
#
# Usage: sudo env FDB_TEST_NFS_EXPORT=<exported directory> bash test_nfs_clients.sh <test binary> <FDB_HOME>
#
#   FDB_TEST_NFS_EXPORT   a directory this host exports through its kernel NFS server (required)
#   FDB_TEST_NFS_CLIENTS  number of clients (default 2)
#   FDB_TEST_NFS_SUBNET   first two octets of the addresses used; client i gets <subnet>.i.2
#                         (default 10.213)
#
# Needs root, iproute2, util-linux and the NFS client and server utilities. Started through sudo,
# the tests themselves run as the invoking user.

set -euo pipefail

die() {
    echo "$(basename "$0"): $*" >&2
    exit 1
}

namespace() { echo "fdb-nfs-client-$1"; }

########################################################################################################################
# Inside the namespaces of one client

client_main() {
    local client=$1 role=$2
    local mount_point="$work/mnt-$client"

    hostname "$(namespace "$client")"
    mount -t nfs -o vers=4.1 "$subnet.$client.1:$export_dir" "$mount_point"

    echo "client $client ($role): $(grep " $mount_point nfs" /proc/self/mounts)"
    stat -c %d "$mount_point" > "$work/device-$client"

    if [[ "$role" == write ]]; then
        # Start together with the other writers: contention is what is being tested.
        touch "$work/ready-$client"
        while [[ ! -e "$work/go" ]]; do
            sleep 0.05
        done
    fi

    # Without FDB_TEST_NFS_MOUNT the single-client cases skip their NFS part.
    local status=0
    runuser -u "$run_as" -- env \
        -u FDB_TEST_NFS_MOUNT \
        -u FDB_TEST_NFS_REQUIRE_MOUNT \
        FDB_HOME="$fdb_home" \
        FDB_TEST_NFS_SHARED_DIR="$mount_point/$shared_name" \
        FDB_TEST_NFS_CLIENTS="$clients" \
        FDB_TEST_NFS_ROLE="$role" \
        "$test_binary" || status=$?

    # Unmount while the server can still be reached: the namespaces are torn down right after.
    umount "$mount_point"
    exit "$status"
}

if [[ "${1:-}" == --client ]]; then
    client_main "$2" "$3"
fi

########################################################################################################################
# On the host

[[ $# -eq 2 ]] || die "usage: sudo env FDB_TEST_NFS_EXPORT=<exported directory> bash $0 <test binary> <FDB_HOME>"
[[ $EUID -eq 0 ]] || die "must run as root: it creates network namespaces and mounts NFS"

test_binary=$(realpath "$1")
fdb_home=$(realpath "$2")
export_dir=$(realpath "${FDB_TEST_NFS_EXPORT:?must name a directory exported by this host}")
clients=${FDB_TEST_NFS_CLIENTS:-2}
subnet=${FDB_TEST_NFS_SUBNET:-10.213}
run_as=${SUDO_USER:-root}

[[ -x "$test_binary" ]] || die "$test_binary is not an executable"
[[ -d "$export_dir" ]] || die "$export_dir is not a directory"
[[ "$clients" =~ ^[0-9]+$ && "$clients" -ge 2 && "$clients" -le 250 ]] || die "FDB_TEST_NFS_CLIENTS must be 2 to 250"

work=$(mktemp -d)
chmod 0755 "$work"
shared_name="fdb-multi-client.$$"
shared_dir="$export_dir/$shared_name"

export work subnet export_dir run_as fdb_home shared_name clients test_binary

cleanup() {
    local client ns
    for ((client = 1; client <= clients; ++client)); do
        ns=$(namespace "$client")
        ip netns pids "$ns" 2> /dev/null | xargs -r kill 2> /dev/null || true
    done
    wait 2> /dev/null || true
    for ((client = 1; client <= clients; ++client)); do
        exportfs -u "$subnet.$client.2:$export_dir" 2> /dev/null || true
        ip netns del "$(namespace "$client")" 2> /dev/null || true
        ip link del "fdbnfs$client" 2> /dev/null || true
    done
    rm -rf "$shared_dir" "$work"
}
trap cleanup EXIT

setup_client() {
    local client=$1
    local ns host_if="fdbnfs$1" client_if="fdbnfs${1}c"
    ns=$(namespace "$client")

    ip netns add "$ns"
    ip link add "$host_if" type veth peer name "$client_if"
    ip link set "$client_if" netns "$ns"
    ip addr add "$subnet.$client.1/24" dev "$host_if"
    ip link set "$host_if" up
    ip -n "$ns" addr add "$subnet.$client.2/24" dev "$client_if"
    ip -n "$ns" link set "$client_if" up
    ip -n "$ns" link set lo up

    exportfs -o rw,sync,no_subtree_check,no_root_squash "$subnet.$client.2:$export_dir"
    mkdir "$work/mnt-$client"
}

# `ip netns exec` also gives the command its own mount namespace, so the NFS mount is seen by this client only.
# The locks block, so a client that never finishes is a failure, not something to wait for.
in_client() {
    timeout --kill-after=10 600 \
        ip netns exec "$(namespace "$1")" unshare --uts -- bash "$0" --client "$1" "$2"
}

show_logs() {
    local log
    for log in "$work"/client-*.log; do
        echo "=== $(basename "$log") ==="
        cat "$log"
    done
}

for ((client = 1; client <= clients; ++client)); do
    setup_client "$client"
done

install -d -m 0777 -o "$run_as" "$shared_dir"

pids=()
for ((client = 1; client <= clients; ++client)); do
    in_client "$client" write > "$work/client-$client.log" 2>&1 &
    pids+=($!)
done

# Release the writers once every client has its mount.
deadline=$((SECONDS + 120))
for ((client = 1; client <= clients; ++client)); do
    until [[ -e "$work/ready-$client" ]]; do
        if ! kill -0 "${pids[client - 1]}" 2> /dev/null || ((SECONDS >= deadline)); then
            show_logs
            die "client $client did not mount $export_dir"
        fi
        sleep 0.1
    done
done
touch "$work/go"

failed=0
for ((client = 1; client <= clients; ++client)); do
    wait "${pids[client - 1]}" || failed=1
done
show_logs
((failed == 0)) || die "at least one client failed"

# One NFS superblock per client: a shared one would mean a shared cache, and a test of nothing.
devices=$(cat "$work"/device-* | sort -u | wc -l)
((devices == clients)) || die "$clients clients used $devices NFS mount(s): they are not independent"

in_client 1 verify

echo "$(basename "$0"): $clients independent NFS clients kept every record of the shared TOC"
