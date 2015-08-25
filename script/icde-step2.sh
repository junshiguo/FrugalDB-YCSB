#!/usr/bin/env bash

step500=10
step50=10
step5=100		
user500=130
user50=0
user5=0
index500=13
index50=0
lbound=0
ubound=0

for ((index500=13; index500>=0; index500--))
do

	((user500=index500*step500))
	((index50=user50/step50))
	((ubound=index50+32))
	((lbound=index50))
	((index50+=16))
	while [ $index50 -ne $lbound ]; do
		((user50=index50*step50))
		echo "lbound="${lbound}" ubound="${ubound}" user50="${user50}" user500="${user500}
		java -Xmx12192m -Xms4096m -jar FClient-icde2.jar -t -db frugaldb.db.FrugalDBClient -P workloada -p table=userl -p recordcount=15000 -p measure=f -p user5=${user5} -p user50=${user50} -p user500=${user500}
		resultcode=$?
		if [ $resultcode -eq 10 ]; then
			((lbound=index50))
		else
			((ubound=index50))
		fi
		((index50=(lbound+ubound)/2))
       		sleep 5
	done
	((user50=lbound*step50))
	echo "**********************************"${user500}","${user50}","${user5}"**********************************"
	printf "\n\n\n"
	
done
		
