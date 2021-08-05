#!/bin/bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash

pushd "`dirname $0`" >/dev/null
es_url=$1
if [ -z "$es_url" ]; then
	echo "# Using localhost:9200 for URL"
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

echo "Deleting templates:"
templates=$(${curl_cmd} -X GET "$es_url/_cat/templates?v")
if echo "${templates}" | grep -q "^curl"; then
    echo ${templates}
else
    for i in $(echo "${templates}" | grep ^cdm | awk '{print $1}'); do
        echo -n "Deleting template $i..."
        qresult=$(${curl_cmd} -X DELETE $es_url/_template/$i)
        if echo "${qresult}" | grep -q '"acknowledged":true'; then
            echo "success"
        else
            echo "failed"
            echo "${qresult}"
        fi
    done
fi

popd >/dev/null
