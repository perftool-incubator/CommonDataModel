#!/bin/bash
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-

# This script will call get-primary-periods.js to find all
# primary periods

project_dir=$(dirname `readlink -e $0`)
pushd "$project_dir" >/dev/null
node ./get-primary-periods.js "$@"
rc=$?
popd >/dev/null
exit $rc
