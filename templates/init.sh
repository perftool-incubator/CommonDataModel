#!/bin/bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash

pushd "`dirname $0`" >/dev/null

es_url=$1
if [ -z "$es_url" ]; then
	es_url="localhost:9200"
fi

if [ ! -x /usr/bin/curl ]; then
	echo "You must have curl installed to use this script"
	exit 1
else
    curl_cmd='curl --silent --show-error --stderr -'
fi

es_ver_file="../VERSION"
if [ -e "$es_ver_file" ]; then
	es_ver=`cat "$es_ver_file"`
else
	echo "Could not find the VERSION file which should be located in the root directory of this repository"
	exit 1
fi

# Build the template and index jsons
make >/dev/null

echo "Initializing ES..."

echo -n "Adding cluster settings..."
# Don't allow index auto-creation
qresult=$(${curl_cmd} -X PUT localhost:9200/_cluster/settings -H 'Content-Type: application/json' -d'
{
  "persistent": {
    "action.auto_create_index": "false",
    "search.max_buckets": 1000000
  }
}
')
if echo "${qresult}" | grep -q '"acknowledged":true'; then
    echo "success"
else
    echo "failed"
    echo "${qresult}"
fi
    #"indices.query.bool.max_clause_count": 1000000,

echo "Checking templates and indices:"
# Create the templates and indices
for i in `/bin/ls *.json | sed -e s/\.json//`; do
    qresult=$(${curl_cmd} -X GET localhost:9200/_cat/templates)
    if echo "${qresult}" | grep -q "$es_ver-$i"; then
        echo "Found template for $i"
    else
        echo -n "Template for $i not found, creating..."
        qresult=$(${curl_cmd} -X PUT $es_url/_template/cdm$es_ver-$i -H 'Content-Type: application/json' -d@./$i.json)
        if echo "${qresult}" | grep -q  '"acknowledged":true'; then
            echo "success"
        else
            echo "failed"
            echo "${qresult}"
        fi
    fi
    qresult=$(${curl_cmd} -X GET localhost:9200/_cat/indices)
    if echo "${qresult}" | grep -q "$es_ver-$i"; then
        echo "Found index for $i"
    else
        echo -n "Index for $i not found, creating..."
        qresult=$(${curl_cmd} -X PUT $es_url/cdm$es_ver-$i)
        if echo "${qresult}" | grep -q '"acknowledged":true'; then
             echo "success"
        else
            echo "failed"
            echo "${qresult}"
        fi
    fi
done

echo "...ES initilization complete!"

popd >/dev/null
