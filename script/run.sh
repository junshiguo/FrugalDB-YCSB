#!/usr/bin/env bash
workloadfile="load_2000.txt"
tests="2 3 4 5"
for testid in $tests
do

	resultfile="icde".$testid
	java -jar PreLoad.jar
	sleep 5
	java -Xmx12192m -Xms4096m -jar FClient.jar -threads 2000 -t -db frugaldb.db.FrugalDBClient -P workloada -p workloadfile_F=$workloadfile -p resultfile_F=$resultfile -testtype frugaldb
	sleep 5
	java -jar CleanVoltdb.jar
	echo "******************************************************"
	sleep 10

done
