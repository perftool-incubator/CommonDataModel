#!/bin/bash
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-

# This script starts the CommonDataModel REST API server
# which provides metric data queries via HTTP endpoints

project_dir=$(dirname `readlink -e $0`)
pushd "$project_dir" >/dev/null
node ./server.js "$@"
rc=$?
popd >/dev/null
exit $rc
