filename="client/instances.txt"
while read -r line
do
	array=(${line//;/ })
	insId="${array[0]}"
	echo "Stopping - $insId"
	aws ec2 stop-instances --instance-ids $insId
done < "$filename"

rm -rf $filename
