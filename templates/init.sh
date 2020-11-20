#!/bin/bash

# This will echo to stdout commands which you can copy/paste to load index templates into ES
# These index templates ensure new indices will adhere to the mappings


pushd "`dirname $0`" >/dev/null

es_url=$1
if [ -z "$es_url" ]; then
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

# Build the template and index jsons
make >/dev/null

# Don't allow index auto-creation
curl -X PUT localhost:9200/_cluster/settings -H 'Content-Type: application/json' -d'
{
  "persistent": {
    "action.auto_create_index": "false"
  }
}
'

# Create the templates and indices
for i in `/bin/ls *.json | sed -e s/\.json//`; do
    curl -X GET localhost:9200/_cat/templates 2>/dev/null | grep -q "$es_ver-$i" || \
        (echo "Template for $i not found, creating"; \
         curl -X PUT $es_url/_template/cdm$es_ver-$i -H 'Content-Type: application/json' -d@./$i.json; \
         echo)
    curl -X GET localhost:9200/_cat/indices 2>/dev/null | grep -q "$es_ver-$i" || \
        (echo "Index for $i not found, creating"; \
	     curl -X PUT $es_url/cdm$es_ver-$i; \
         echo)
done

popd >/dev/null
