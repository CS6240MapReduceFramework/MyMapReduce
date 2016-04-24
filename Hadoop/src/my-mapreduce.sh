#!/bin/bash
keyPair="kaushikfinaaws"

make all
cd server/$1 && sbt assembly && mv target/scala-*/$1*.jar ../../server.jar
cd ../..
pseudoDistributed=$4

if [ $pseudoDistributed == "distributed" ]; then

    echo "Running in Distributed Mode";
	i=0
	while read -r line;
	do
		if [ $i -gt 0 ]; then
			array=(${line//;/ })
			insIp="${array[1]}"
			echo "copying files"
			scp -i $keyPair.pem -o StrictHostKeyChecking=no server.jar ec2-user@$insIp:~
			scp -i $keyPair.pem config.properties ec2-user@$insIp:~
			scp -i $keyPair.pem instances.txt ec2-user@$insIp:~
			scp -i $keyPair.pem $keyPair.pem ec2-user@$insIp:~
			ssh -i $keyPair.pem ec2-user@$insIp "java -jar server.jar > log.txt" &
		fi
		i=$((i+1))
	done < "instances.txt"
	sleep 10
	echo "executing jar"
	cd client && java -jar client.jar $2 $3

else

	echo "Running in Pseudo Mode";

	while read -r line; 
	do
		echo $line
		noOfInst=$((line))
	done < "instances.txt"

	port=3001
	i=0
	
	while [ $i -lt $noOfInst ];
    do
		echo "killing process in port: "$port
        pid=$(lsof -i:$port -t); kill -TERM $pid || kill -KILL $pid
        echo "pseudo;127.0.1.1;"$port>>instances.txt
        java -jar server.jar > server_$port.txt &
        port=$((port+1))
        i=$((i+1))
		echo "=============================================="
		sleep 50
    done
	
	cp instances.txt client/
	cd client && java -jar client.jar $2 $3

fi
