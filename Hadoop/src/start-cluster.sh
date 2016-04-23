#!/bin/bash
secGroup="launch-wizard-1"
keyPair="CCS_AWS_Key"

noOfInst=$1
pseudoDistributed=$2
declare -a instancesArray

echo "creating "$noOfInst" instances"
touch instances.txt
echo $noOfInst>instances.txt
i=0

# Distributed Mode
if [ $pseudoDistributed == "distributed" ]; then
    echo "Running in Distributed Mode";
    while [ $i -lt $noOfInst ];
    do
        instancesArray[$i]=$(aws ec2 run-instances --image-id ami-08111162 --security-group-ids $secGroup --count 1 --instance-type t2.micro --key-name $keyPair --query 'Instances[0].InstanceId')
        i=$((i+1))
		echo "instance "$i" created"
    done

    echo "sleep 100"
    sleep 100
    i=0
    while [ $i -lt $noOfInst ];
    do
    	insId=${instancesArray[i]}
    	insId="${insId%\"}"
    	insId="${insId#\"}"

    	insIp=$(aws ec2 describe-instances --instance-ids $insId --query 'Reservations[0].Instances[0].PublicIpAddress')

    	insIp="${insIp%\"}"
    	insIp="${insIp#\"}"
    	echo $insId";"$insIp";3002">>instances.txt

    	i=$((i+1))
    	echo "---------------------------"
    done

#Pseudo Mode
else
    echo "Running in Pseudo Mode";
    port=3000
    i=0
    NET_IP=`ifconfig ${NET_IF} | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'`
    while [ $i -lt $noOfInst ];
    do
        echo "psuedo;"$NET_IP";"$port>>instances.txt

        echo "executing jar"
       	ssh $NET_IP "java -jar server.jar > log.txt" &

        port=$((port+1))
        i=$((i+1))
    done
fi

mv instances.txt client/instances.txt

