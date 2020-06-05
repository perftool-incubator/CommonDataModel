#!/bin/bash

# This will echo to stdout commands which you can copy/paste to load index templates into ES
# These index templates ensure new indices will adhere to the mappings

es_url=$1
if [ -z "$es_url" ]; then
	echo "You must provide the URL to your Elasticsearch instance"
	exit 1
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


for i in `/bin/ls *.json | sed -e s/\.json//`; do
	echo "curl -X PUT $es_url/_template/cdm$es_ver-$i -H 'Content-Type: application/json' -d@./$i.json"
done
echo
