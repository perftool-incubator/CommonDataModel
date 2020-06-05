#!/bin/bash

ver=$1
shift
index=$1
file=$index.json
echo '{' >$file
echo '  "index_patterns": ["cdm'$ver'-'$index'*"],' >>$file
echo '  "settings": {' >>$file
echo '    "number_of_shards": 96,' >>$file
echo '    "number_of_replicas": 2,' >>$file
echo '    "codec" : "best_compression",' >>$file
echo '    "refresh_interval" : "5s"' >>$file
echo '  },' >>$file
echo '  "mappings": {' >>$file
echo '    "'$index'": {' >>$file
echo '      "dynamic": "strict",' >>$file
echo '      "properties": {' >>$file
if [ "$index" != "metric_samp" ]; then
	echo '        "cdm": {' >>$file
	echo '          "properties": {' >>$file
	echo '            "ver": { "type": "integer" },' >>$file
	echo '            "doctype": { "type": "keyword" }' >>$file
	echo '          }' >>$file
	echo '        },' >>$file
fi
while [ ! -z "$1" ]; do
    cat $1.base >> $file
    shift
done
echo "      }" >> $file
echo "    }" >> $file
echo "  }" >> $file
echo "}" >> $file
