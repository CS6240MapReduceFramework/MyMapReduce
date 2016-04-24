#!/bin/bash
i=0
while read -r line
do
	if [ $i -gt 0 ]; then
		array=(${line//;/ })
		insId="${array[0]}"
		echo "Stopping - $insId"
		if [ $insId != "pseudo" ]; then
			aws ec2 stop-instances --instance-ids $insId
		fi
	fi
	i=$((i+1))
done < "instances.txt"
sleep 10
rm -rf instances.txt client/instances.txt
