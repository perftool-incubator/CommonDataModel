#!/bin/bash

# This will echo commands on stdout that you can copy/paste to clean up your ES instance

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

for i in `curl --stderr /dev/null -X GET "$es_url/_cat/indices?" | awk '{print $3}'`; do
	echo curl --stderr /dev/null -X DELETE $es_url/$i
done

for i in `curl --stderr /dev/null -X GET "$es_url/_cat/templates?v" | grep cdm$es_ver | awk '{print $1}'`; do
	echo curl --stderr /dev/null -X DELETE localhost:9201/_template/$i; done

