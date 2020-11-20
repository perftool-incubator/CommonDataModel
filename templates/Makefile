
templates	: run.json tag.json iteration.json param.json sample.json period.json metric_desc.json metric_data.json

run.json	: run.base
		  ./build.sh `cat ../VERSION` run

tag.json	: run.base tag.base
		  ./build.sh `cat ../VERSION` tag run

iteration.json	: run.base iteration.base
		  ./build.sh `cat ../VERSION` iteration run

param.json	: run.base iteration.base param.base
		  ./build.sh `cat ../VERSION` param iteration run

sample.json	: run.base iteration.base sample.base
		  ./build.sh `cat ../VERSION` sample iteration run

period.json	: run.base iteration.base sample.base period.base
		  ./build.sh `cat ../VERSION` period sample iteration run

metric_desc.json	: run.base iteration.base sample.base period.base metric_desc.base
		  ./build.sh `cat ../VERSION` metric_desc period sample iteration run

metric_data.json	: metric_data.base
		  ./build.sh `cat ../VERSION` metric_data

clean		: 
		  /bin/rm -f *.json
