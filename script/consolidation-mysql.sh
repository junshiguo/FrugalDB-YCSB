#!/usr/bin/env bash

totalTenant=400
ubound=600
lbound=100

while [ $totalTenant -ne $lbound ]; do
	workloadfile=load_${totalTenant}".txt"
	resultfile=load_${totalTenant}
	java -jar WGConsolidation.jar $totalTenant 1 1
	java -Xmx12192m -Xms4096m -jar FClient.jar -threads ${totalTenant} -t -db frugaldb.db.FrugalDBClient -P workloada -p workloadfile_F=$workloadfile -p resultfile_F=$resultfile -testtype mysql
	resultcode=$?
	if [ $resultcode -eq 10 ]; then
		((lbound=totalTenant))
	else
		((ubound=totalTenant))
	fi
	((totalTenant=(lbound+ubound)/2))

done
