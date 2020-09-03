#!/bin/bash

pushd "`dirname $0`" >/dev/null
es_url=$1
if [ -z "$es_url" ]; then
	echo "# Using localhost:9200 for URL"
	es_url="localhost:9200"
fi

if [ ! -x /usr/bin/curl ]; then
	echo "You must have curl installed to use this script"
	exit 1
fi

es_ver_file="../VERSION"
if [ -e "$es_ver_file" ]; then
	es_ver=`cat "$es_ver_file"`
else
	echo "Could not find the VERSION file which should be located in the root directory of this repository"
	exit 1
fi

echo "Deleting indices"
for i in `curl --stderr /dev/null -X GET "$es_url/_cat/indices?" | awk '{print $3}'`; do
    echo "Deleting index $i"
	curl --stderr /dev/null -X DELETE $es_url/$i
    echo
done

echo "Deleting templates"
for i in `curl --stderr /dev/null -X GET "$es_url/_cat/templates?v" | grep cdm$es_ver | awk '{print $1}'`; do
    echo "Deleting template $i"
	curl --stderr /dev/null -X DELETE $es_url/_template/$i
    echo
done
popd >/dev/null
