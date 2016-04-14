secGroup="my-sg"
keyPair="SudeepMR"
noOfInst=$1

echo "creating "$noOfInst" instances"
touch instances.txt
echo $noOfInst>instances.txt
i=0

while [ $i -lt $noOfInst ];
do
	insId=$(aws ec2 run-instances --image-id ami-08111162 --security-group-ids $secGroup --count 1 --instance-type m3.medium --key-name $keyPair --query 'Instances[0].InstanceId')
	sleep 100

	insId="${insId%\"}"
	insId="${insId#\"}"

	insIp=$(aws ec2 describe-instances --instance-ids $insId --query 'Reservations[0].Instances[0].PublicIpAddress')

	insIp="${insIp%\"}"
	insIp="${insIp#\"}"

	echo $insId";"$insIp>>instances.txt
	echo "scp -i $keyPair.pem instances.txt ec2-user@$insIp:~"
	scp -i $keyPair.pem instances.txt ec2-user@$insIp:~
	scp -i $keyPair.pem server.jar ec2-user@$insIp:~
	echo "********executing jar"

	ssh -i $keyPair.pem ec2-user@$insIp "java -jar server.jar > log.txt" &

	i=$((i+1))
	echo "---------------------------"
done

