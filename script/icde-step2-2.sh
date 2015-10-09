#!/usr/bin/env bash

index500=([0]=0 [1]=10 [2]=20 [3]=30 [4]=40 [5]=45)
points[0]="480 450 400 350 300 250 200 150 100 50 0"
points[1]="250 200 150 100 50 0"
points[2]="120 100 50 0"
points[3]="40 0"
points[4]="20 0"
points[5]="10 0"
step500=10
step50=50
step5=50		
user5=0
index5=0
lbound=0
ubound=32

for ((i=5; i>=0; i--))
do

	user500=${index500[${i}]}
	user5=0
	for user50 in ${points[${i}]}; do
		((index5=user5/step5))
		((ubound=index5+32))
		((lbound=index5))
		((index5+=16))
		while [ $index5 -ne $lbound ]; do
			((user5=index5*step5))
			echo "lbound="${lbound}" ubound="${ubound}" user500="${user500}" user50="${user50}" user5="${user5}
			java -Xmx12192m -Xms4096m -jar FClient-icde2.jar -t -db frugaldb.db.FrugalDBClient -P workloada -p table=users -p recordcount=5000 -p measure=f -p user5=${user5} -p user50=${user50} -p user500=${user500}
			resultcode=$?
			if [ $resultcode -eq 10 ]; then
				((lbound=index5))
			else
				((ubound=index5))
			fi
			((index5=(lbound+ubound)/2))
	       		sleep 5
		done
		((user5=lbound*step5))
		echo "**********************************"${user500}","${user50}","${user5}"**********************************"
		printf "\n\n\n"
	done

done

