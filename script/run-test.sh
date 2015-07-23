#!/usr/bin/env bash
pre[0]="550500_0.25_test"
pre[1]="550500_0.30_test"
pre[2]="550500_0.35_test"
pre[3]="550500_0.40_test"

tests="1 2 3 4 5"

for testid in $tests
do
	workloadfile=${pre[0]}.testid
	resultfile=$workloadfile."_mysql"
	java -Xmx12192m -Xms4096m -jar FClient.jar -threads 2000 -t -db frugaldb.db.FrugalDBClient -P workloada -p workloadfile_F=$workloadfile -p resultfile_F=$resultfile -testtype mysql
	echo "**************************************************"
	sleep 30
	resultfile=$workloadfile."_frugaldb-2000"
	java -Xmx12192m -Xms4096m -jar FClient.jar -threads 2000 -t -db frugaldb.db.FrugalDBClient -P workloada -p workloadfile_F=$workloadfile -p resultfile_F=$resultfile -testtype frugaldb
	echo "**************************************************"
	sleep 30
	java -jar CleanVoltdb.jar
	sleep 10
done

#java -Xmx12192m -Xms4096m -jar FClient.jar -threads 2000 -t -db frugaldb.db.FrugalDBClient -P workloada -testtype mysql -p workloadfile_F=$workloadfile -p resultfile_F=$workloadfile+"_mysql"

