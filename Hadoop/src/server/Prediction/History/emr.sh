## Author: Habiba Neelesh
#!/bin/bash
echo "Running Script!"

## EMR - Hadoop
gradle build
cp build/libs/*.jar Main.jar
aws s3 mb s3://neel-habiba/
aws s3 cp Main.jar s3://neel-habiba/
aws s3 cp all s3://neel-habiba/input --recursive
aws s3 rm s3://neel-habiba/output --recursive
rm -rf output
mkdir output
aws emr create-cluster --name "HW4_Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge --steps Type=CUSTOM_JAR,Name="Main program JAR",ActionOnFailure=CONTINUE,Jar=s3://neel-habiba/Main.jar,MainClass=Main,Args=[s3://neel-habiba/input,s3://neel-habiba/output] --auto-terminate --log-uri s3://neel-habiba/logs --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging > ~/.aws/clusterID

aws emr describe-cluster --cluster-id  $(cat ~/.aws/clusterID| jq -r .ClusterId)| jq -r .Cluster.Status.State
echo 'Creating a cluster.. wait for the end message..';
i=0
while [[ "$(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq '.Cluster.Status.State')" != "\"TERMINATED\""  &&  "$(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq '.Cluster.Status.State')" != "\"TERMINATING\"" ]]
do
  echo -ne $(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq -r '.Cluster.Status.State');
done
s3cmd get s3://neel-habiba/output/* output/
cd ../..





