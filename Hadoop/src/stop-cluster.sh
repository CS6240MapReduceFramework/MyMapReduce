#!/bin/bash
filename="client/instances.txt"
i=0
while read -r line
do
	if [ $i -gt 0 ]; then
		array=(${line//;/ })
		insId="${array[0]}"
		echo "Stopping - $insId"
		aws ec2 stop-instances --instance-ids $insId
	fi
	i=$((i+1))
done < "$filename"

rm -rf $filename
