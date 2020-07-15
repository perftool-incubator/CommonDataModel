#!/bin/bash

ver=$1
shift
index=$1
file=$index.json
echo '{' >$file
echo '  "index_patterns": ["cdm'$ver'-'$index'*"],' >>$file
#echo '  "template": {' >>$file
echo '  "settings": {' >>$file
echo '    "number_of_shards": 2,' >>$file
echo '    "number_of_replicas": 1,' >>$file
echo '    "codec" : "best_compression",' >>$file
echo '    "refresh_interval" : "5s"' >>$file
echo '  },' >>$file
echo '  "mappings": {' >>$file
#echo '      "'$index'": {' >>$file
#echo '      "'_doc'": {' >>$file
echo '    "dynamic": "strict",' >>$file
echo '    "properties": {' >>$file
#if [ "$index" != "metric_data" ]; then
	echo '        "cdm": {' >>$file
	echo '          "properties": {' >>$file
	echo '            "ver": { "type": "keyword" },' >>$file
	echo '            "doctype": { "type": "keyword" }' >>$file
	echo '          }' >>$file
	echo '        },' >>$file
#fi
while [ ! -z "$1" ]; do
    cat $1.base >> $file
    shift
done
#echo "        }" >> $file
#echo "      }" >> $file
echo "    }" >> $file
echo "  }" >> $file
echo "}" >> $file
