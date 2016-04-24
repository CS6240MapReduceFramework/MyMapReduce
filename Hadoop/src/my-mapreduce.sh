#!/bin/bash
keyPair="kaushikfinaaws"
make all
cd server/$1 && sbt assembly && mv target/scala-*/$1*.jar ../../server.jar
cd ../..

i=0;
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
		echo "executing jar"
		ssh -i $keyPair.pem ec2-user@$insIp "java -jar server.jar > log.txt" &
	fi
	i=$((i+1))
done < "instances.txt"
sleep 10
cd client && java -jar client.jar $2 $3
