secGroup="launch-wizard-1"
keyPair="CCS_AWS_Key"
noOfInst=$1
declare -a instancesArray

echo "creating "$noOfInst" instances"
touch instances.txt
echo $noOfInst>instances.txt
i=0

while [ $i -lt $noOfInst ];
do
	instancesArray[$i]=$(aws ec2 run-instances --image-id ami-08111162 --security-group-ids $secGroup --count 1 --instance-type t2.micro --key-name $keyPair --query 'Instances[0].InstanceId')
	i=$((i+1))
done


sleep 100
i=0

while [ $i -lt $noOfInst ];
do
	insId=${instancesArray[i]}
	echo $insId
	insId="${insId%\"}"
	insId="${insId#\"}"

	insIp=$(aws ec2 describe-instances --instance-ids $insId --query 'Reservations[0].Instances[0].PublicIpAddress')

	insIp="${insIp%\"}"
	insIp="${insIp#\"}"

	echo $insId";"$insIp>>instances.txt
	echo "scp -i $keyPair.pem instances.txt ec2-user@$insIp:~"
	scp -i $keyPair.pem server.jar ec2-user@$insIp:~
	scp -i $keyPair.pem config.properties ec2-user@$insIp:~
	scp -i $keyPair.pem $keyPair.pem ec2-user@$insIp:~
	echo "********executing jar"

	ssh -i $keyPair.pem ec2-user@$insIp "java -jar server.jar > log.txt" &

	i=$((i+1))
	echo "---------------------------"
done

mv instances.txt client/instances.txt

