#!/bin/bash

ver=$1
shift
index=$1
file=$index.json
echo '{' >$file
echo '  "index_patterns": ["cdm'$ver'-'$index'*"],' >>$file
echo '  "settings": {' >>$file
echo '    "number_of_shards": 8,' >>$file
echo '    "number_of_replicas": 1,' >>$file
echo '    "max_result_window": 262144,' >>$file
echo '    "max_terms_count": 262144,' >>$file
echo '    "codec" : "best_compression",' >>$file
echo '    "refresh_interval" : "5s"' >>$file
echo '  },' >>$file
echo '  "mappings": {' >>$file
#if [ "$index" != "metric_desc" ]; then
    echo '    "dynamic": "strict",' >>$file
#fi
echo '    "properties": {' >>$file
echo '        "cdm": {' >>$file
echo '          "properties": {' >>$file
echo '            "ver": { "type": "keyword" },' >>$file
echo '            "doctype": { "type": "keyword" }' >>$file
echo '          }' >>$file
echo '        },' >>$file
while [ ! -z "$1" ]; do
    cat $1.base >> $file
    shift
done
echo "    }" >> $file
echo "  }" >> $file
echo "}" >> $file
