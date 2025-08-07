#!/bin/bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash

pushd "`dirname $0`" >/dev/null

# Only local Opensearch is allowed to be deleted
echo "# Using localhost:9200 for URL"
es_url="localhost:9200"


if [ ! -x /usr/bin/curl ]; then
    echo "You must have curl installed to use this script"
    exit 1
else
    curl_cmd='curl --silent --show-error --stderr -'
fi

echo "Deleting indices:"
indices=$(${curl_cmd} -X GET "$es_url/_cat/indices?")
if echo "${indices}" | grep -q "^curl"; then
    echo ${indices}
else
    for i in $(echo "${indices}" | awk '{print $3}'); do
        echo -n "Deleting index $i..."
        qresult=$(${curl_cmd} -X DELETE $es_url/$i)
        if echo "${qresult}" | grep -q '"acknowledged":true'; then
            echo "success"
        else
            echo "failed"
            echo "${qresult}"
        fi
    done
fi


popd >/dev/null
