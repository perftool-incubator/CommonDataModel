#!/bin/bash
# Wait for node_modules to be installed by start-server.sh.
# Source this from query scripts before invoking node.
# The CDM server (start-server.sh) runs npm ci and creates
# node_modules/.install-stamp when done. Query scripts run in
# separate containers that share the same bind-mounted filesystem,
# so they just need to wait for the stamp file to appear.

_cdmq_wait_for_deps() {
    local max_wait=120
    local waited=0
    if [ -f "node_modules/.install-stamp" ]; then
        return 0
    fi
    echo "Waiting for cdmq dependencies (up to ${max_wait}s)..." >&2
    while [ $waited -lt $max_wait ]; do
        if [ -f "node_modules/.install-stamp" ]; then
            echo "Dependencies ready (waited ${waited}s)." >&2
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    echo "Error: node_modules/.install-stamp not found after ${max_wait}s." >&2
    echo "The CDM server (start-server.sh) may not be running." >&2
    echo "Start it with 'crucible start opensearch' or run 'npm install' manually in $(pwd)." >&2
    return 1
}
