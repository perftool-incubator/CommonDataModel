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
    echo "Current CDM version is $es_ver"
else
	echo "Could not find the VERSION file which should be located in the root directory of this repository"
	exit 1
fi

# Build the template and index jsons
make clean >/dev/null
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
    echo "Checking for cdm${es_ver}-${i}:"
    qresult=$(${curl_cmd} -X GET localhost:9200/_cat/templates)
    if echo "${qresult}" | grep -q "$es_ver-$i"; then
        echo "  found template"
    else
        echo -n "  template missing, creating..."
        qresult=$(${curl_cmd} -X PUT $es_url/_template/cdm$es_ver-$i -H 'Content-Type: application/json' -d@./$i.json)
        if echo "${qresult}" | grep -q  '"acknowledged":true'; then
            echo "success"
        else
            echo "failed"
            echo "${qresult}"
        fi
    fi
    older_templates=$(${curl_cmd} -X GET localhost:9200/_cat/templates | grep ^cdm | grep -- -$i | grep -v $es_ver | awk '{print $1}')
    if [ ! -z "$older_templates" ]; then
        for this_older_template in $older_templates; do
            older_ver=`echo $this_older_template | sed -e s/^cdm// | awk -F- '{print $1}'`
            echo -n "  found older version ($older_ver) of this template, deleteing..."
            qresult=$(${curl_cmd} -X DELETE $es_url/_template/$this_older_template)
            if echo "${qresult}" | grep -q  '"acknowledged":true'; then
                echo "success"
            else
                echo "failed"
                echo "${qresult}"
            fi
        done
    fi

    qresult=$(${curl_cmd} -X GET localhost:9200/_cat/indices)
    if echo "${qresult}" | grep -q "$es_ver-$i"; then
        echo "  found index"
    else
        echo -n "  index missing, creating..."
        create_cmd="${curl_cmd} -X PUT $es_url/cdm$es_ver-$i"
        echo -n " $create_cmd "
        qresult=$($create_cmd)
        if echo "${qresult}" | grep -q '"acknowledged":true'; then
             echo "success"
        else
            echo "failed"
            echo "${qresult}"
        fi
    fi
    older_indices=$(${curl_cmd} -X GET localhost:9200/_cat/indices | awk '{print $3}' | grep ^cdm | grep -- -$i | grep -v $es_ver | awk '{print $1}')
    if [ ! -z "$older_indices" ]; then
        for this_older_index in $older_indices; do
            older_ver=`echo $this_older_index | sed -e s/^cdm// | awk -F- '{print $1}'`
            echo -n "  found older version ($older_ver) of this index, deleting..."
            qresult=$(${curl_cmd} -X DELETE $es_url/$this_older_index)
            if echo "${qresult}" | grep -q  '"acknowledged":true'; then
                echo "success"
            else
                echo "failed"
                echo "${qresult}"
            fi
        done
    fi
done

echo "...ES initilization complete!"

popd >/dev/null
