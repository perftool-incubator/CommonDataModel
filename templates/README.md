## Introduction
The files in this directory are for taking a newly created instance of Elastcisearch and creating the indices used for CommonDataModel

## Directory Contents
In this directory are "base" json files, a Makefile to generate json files, and some scripts that generate commands to configure ES.

The Makefile uses build.sh to combine one of more base files with a json header to produce an index template for ES.  There is an index template for each document type used in CommonDataModel.  After running "make", these files will be generated:

    iteration.json
    metric_data.json
    metric_desc.json
    period.json
    run.json
    sample.json

There are also thress scripts which generate curl commands to help you create and cleanup the elasticsearch instance:

    create-cleanup-cmds.sh
    create-indices-cmds.sh
    create-template-cmds.sh

## Creating the CommonDataModel Indices
These instructions assume you already have an elastixsearch instance installed and reachable via localhost.  Adjust the URL accrodingly for a instance at a different location.

First run 'make'

    $ make
    ./build.sh `cat ../VERSION` run
    ./build.sh `cat ../VERSION` iteration run
    ./build.sh `cat ../VERSION` sample iteration run
    ./build.sh `cat ../VERSION` period sample iteration run
    ./build.sh `cat ../VERSION` metric_desc period sample iteration run
    ./build.sh `cat ../VERSION` metric_data
    ls -l *.json
    -rw-rw-r--. 1 atheurer atheurer 1570 Jun 29 13:18 iteration.json
    -rw-rw-r--. 1 atheurer atheurer  808 Jun 29 13:18 metric_data.json
    -rw-rw-r--. 1 atheurer atheurer 4212 Jun 29 13:18 metric_desc.json
    -rw-rw-r--. 1 atheurer atheurer 2030 Jun 29 13:18 period.json
    -rw-rw-r--. 1 atheurer atheurer 1256 Jun 29 13:18 run.json
    -rw-rw-r--. 1 atheurer atheurer 1727 Jun 29 13:18 sample.json

Now run create-template-cmds.sh

    $ ./create-template-cmds.sh localhost:9200
    curl -X PUT localhost:9200/_template/cdmv5dev-iteration -H 'Content-Type: application/json' -d@./iteration.json
    curl -X PUT localhost:9200/_template/cdmv5dev-metric_data -H 'Content-Type: application/json' -d@./metric_data.json
    curl -X PUT localhost:9200/_template/cdmv5dev-metric_desc -H 'Content-Type: application/json' -d@./metric_desc.json
    curl -X PUT localhost:9200/_template/cdmv5dev-period -H 'Content-Type: application/json' -d@./period.json
    curl -X PUT localhost:9200/_template/cdmv5dev-run -H 'Content-Type: application/json' -d@./run.json
    curl -X PUT localhost:9200/_template/cdmv5dev-sample -H 'Content-Type: application/json' -d@./sample.json

You will then need to copy/paste those commands to your prompt to run them:

    $ curl -X PUT localhost:9200/_template/cdmv5dev-iteration -H 'Content-Type: application/json' -d@./iteration.json
    {"acknowledged":true}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/_template/cdmv5dev-metric_data -H 'Content-Type: application/json' -d@./metric_data.json
    {"acknowledged":true}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/_template/cdmv5dev-metric_desc -H 'Content-Type: application/json' -d@./metric_desc.json
    {"acknowledged":true}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/_template/cdmv5dev-period -H 'Content-Type: application/json' -d@./period.json
    {"acknowledged":true}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/_template/cdmv5dev-run -H 'Content-Type: application/json' -d@./run.json
    {"acknowledged":true}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/_template/cdmv5dev-sample -H 'Content-Type: application/json' -d@./sample.json

Then confirm the templates have been created:

    $ curl -X GET localhost:9200/_cat/templates?v | grep cdm
      % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                     Dload  Upload   Total   Spent    Left  Speed
    100  2142  100  2142    0     0   697k      0 --:--:-- --:--:-- --:--:--  697k
    cdmv5dev-run                    [cdmv5dev-run*]              0
    cdmv5dev-metric_data            [cdmv5dev-metric_data*]      0
    cdmv5dev-sample                 [cdmv5dev-sample*]           0
    cdmv4dev-run                    [cdmv4dev-run*]              0
    cdmv5dev-metric_desc            [cdmv5dev-metric_desc*]      0
    cdmv5dev-iteration              [cdmv5dev-iteration*]        0
    cdmv5dev-period                 [cdmv5dev-period*]           0

Then run the command to generate the create-index commands:

    $ ./create-indices-cmds.sh  localhost:9200
    curl -X PUT localhost:9200/cdmv5dev-iteration
    curl -X PUT localhost:9200/cdmv5dev-metric_data
    curl -X PUT localhost:9200/cdmv5dev-metric_desc
    curl -X PUT localhost:9200/cdmv5dev-period
    curl -X PUT localhost:9200/cdmv5dev-run
    curl -X PUT localhost:9200/cdmv5dev-sample

And like before, you will need to copy/paste these to run those commands:

    $ curl -X PUT localhost:9200/cdmv5dev-iteration
    {"acknowledged":true,"shards_acknowledged":true,"index":"cdmv5dev-iteration"}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/cdmv5dev-metric_data
    {"acknowledged":true,"shards_acknowledged":true,"index":"cdmv5dev-metric_data"}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/cdmv5dev-metric_desc
    {"acknowledged":true,"shards_acknowledged":true,"index":"cdmv5dev-metric_desc"}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/cdmv5dev-period
    {"acknowledged":true,"shards_acknowledged":true,"index":"cdmv5dev-period"}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/cdmv5dev-run
    {"acknowledged":true,"shards_acknowledged":true,"index":"cdmv5dev-run"}[atheurer@dhcp31-123 templates]$ curl -X PUT localhost:9200/cdmv5dev-sample

And then confirm those indices exist:

    $ curl -X GET localhost:9200/_cat/indices?v
    health status index                uuid                   pri rep docs.count docs.deleted store.size pri.store.size
    yellow open   cdmv5dev-iteration   TVp86-vGSD6IFcsL1VV2Yg   1   1          0            0       230b           230b
    yellow open   cdmv5dev-metric_desc XmpbfBhNRAKLyHVmBkr4Pw   1   1          0            0       230b           230b
    yellow open   cdmv5dev-metric_data h4M_jhnMTDu5vgsxgpUQqw   1   1          0            0       230b           230b
    yellow open   cdmv5dev-period      P5pSc2yoRPiKVEAhc5RJLQ   1   1          0            0       230b           230b
    yellow open   cdmv5dev-run         Ng4dssP2Rr-uSuo3tZC8Pw   1   1          0            0       230b           230b

At this point the elasticsearch instance should be ready to consume documents for CommonDataModel

If you wish to start over with a cleared out elasticsearch instances, you can run this command which outputs commands to do so:

    $ ./create-cleanup-cmds.sh  localhost:9200
    curl --stderr /dev/null -X DELETE localhost:9200/cdmv5dev-iteration
    curl --stderr /dev/null -X DELETE localhost:9200/cdmv5dev-metric_desc
    curl --stderr /dev/null -X DELETE localhost:9200/cdmv5dev-metric_data
    curl --stderr /dev/null -X DELETE localhost:9200/cdmv5dev-period
    curl --stderr /dev/null -X DELETE localhost:9200/cdmv5dev-run
    curl --stderr /dev/null -X DELETE localhost:9201/_template/cdmv5dev-run
    curl --stderr /dev/null -X DELETE localhost:9201/_template/cdmv5dev-metric_data
    curl --stderr /dev/null -X DELETE localhost:9201/_template/cdmv5dev-sample
    curl --stderr /dev/null -X DELETE localhost:9201/_template/cdmv5dev-metric_desc
    curl --stderr /dev/null -X DELETE localhost:9201/_template/cdmv5dev-iteration
    curl --stderr /dev/null -X DELETE localhost:9201/_template/cdmv5dev-period

And then you can copy/paste those commands to clear out the instance.
