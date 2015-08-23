#!/usr/bin/env bash

totalTenant=2500
ubound=3000
lbound=1000

while [ $totalTenant -ne $lbound ]; do
	workloadfile=load_${totalTenant}".txt"
	resultfile=load_${totalTenant}
	java -jar WGConsolidation.jar $totalTenant 0.25 0
	java -Xmx12192m -Xms4096m -jar FClient.jar -threads ${totalTenant} -t -db frugaldb.db.FrugalDBClient -P workloada -p workloadfile_F=$workloadfile -p resultfile_F=$resultfile -testtype frugaldb
	resultcode=$?
	if [ $resultcode -eq 10 ]; then
		((lbound=totalTenant))
	else
		((ubound=totalTenant))
	fi
	((totalTenant=(lbound+ubound)/2))
	
	sleep 10
	java -jar CleanVoltdb.jar
	sleep 10 

done
