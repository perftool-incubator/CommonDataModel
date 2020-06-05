
templates	: run.json config_cpu_vulnerabilities.json config_yum_repos.json iteration.json sample.json period.json metric_desc.json metric_data.json
		  ls -l *.json

run.json	: run.base
		  ./build.sh `cat ../VERSION` run

config_cpu_vulnerabilities.json	: run.base config_yum_repos.base config_cpu_vulnerabilities.base
		  ./build.sh `cat ../VERSION` config_cpu_vulnerabilities run

config_yum_repos.json	: run.base config_yum_repos.base
		  ./build.sh `cat ../VERSION` config_yum_repos run

iteration.json	: run.base iteration.base
		  ./build.sh `cat ../VERSION` iteration run

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
