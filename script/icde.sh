#!/usr/bin/env bash

tablename[0]="users"
tablename[1]="userm"
tablename[2]="userl"
recordcounts[0]="5000"
recordcounts[1]="10000"
recordcounts[2]="15000"

tests="0 1 2"
operationcountss="5 50 500"
for testid in ${tests}
do
	for operationcounts in ${operationcountss} 
	do
		
		totalTenant=1000
		ubound=3000
		lbound=10
		while [ $totalTenant -ne $lbound ]; do
			java -Xmx12192m -Xms4096m -jar FClient-icde.jar -threads ${totalTenant} -t -db frugaldb.db.FrugalDBClient -P workloada -p table=${tablename[${testid}]} -p recordcount=${recordcounts[${testid}]} -p operationcount=${operationcounts} -p measure=f
			resultcode=$?
			if [ $resultcode -eq 10 ]; then
				((lbound=totalTenant))
			else
				((ubound=totalTenant))
			fi
			((totalTenant=(lbound+ubound)/2))
			echo "lbound="${lbound}" ubound="${ubound}" totalTenant="${totalTenant}
			echo ${tablenames[${testid}]}" "${operationcounts} > consolidation.txt
			echo "lbound="${lbound}" ubound="${ubound}" totalTenant="${totalTenant} > consolidation.txt
			echo -e "\n" > consolidation.txt
       			sleep 10
		done
		echo "**********************************"${lbound}"***************************************"
		printf "\n\n\n\n"

	
	done
done
