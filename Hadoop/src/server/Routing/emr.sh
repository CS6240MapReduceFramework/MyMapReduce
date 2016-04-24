## Author: Habiba Neelesh
#!/bin/bash
echo "Running Script!"

## EMR - Hadoop
hadoop com.sun.tools.javac.Main Main.java
jar cf main.jar Main*.class

hadoop com.sun.tools.javac.Main MapReduce2.java
jar cf map2.jar Main*.class

aws s3 mb s3://neel-habiba/
aws s3 cp main.jar s3://neel-habiba/
aws s3 cp map2.jar s3://neel-habiba/

aws s3 cp a7history s3://neel-habiba/a7history --recursive
aws s3 cp a7test s3://neel-habiba/a7test --recursive

aws s3 rm s3://neel-habiba/history --recursive
aws s3 rm s3://neel-habiba/connections --recursive

rm -rf input/history
mkdir input/history

aws emr create-cluster --name "HW7_Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge \
--steps Type=CUSTOM_JAR,Name="Main program JAR",ActionOnFailure=CONTINUE,Jar=s3://neel-habiba/main.jar,MainClass=Main,Args=[s3://neel-habiba/a7history,s3://neel-habiba/history] \
--auto-terminate --log-uri s3://neel-habiba/logs --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging > ~/.aws/clusterID

aws emr describe-cluster --cluster-id  $(cat ~/.aws/clusterID| jq -r .ClusterId)| jq -r .Cluster.Status.State
echo 'Creating a cluster.. wait for the end message..';
i=0
while [[ "$(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq '.Cluster.Status.State')" != "\"TERMINATED\""  &&  "$(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq '.Cluster.Status.State')" != "\"TERMINATING\"" ]]
do
  echo -ne $(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq -r '.Cluster.Status.State');
done
s3cmd get s3://neel-habiba/history/* input/history/

rm -rf input/connections
mkdir input/connections

aws emr create-cluster --name "HW7_Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge \
--steps Type=CUSTOM_JAR,Name="MapReduce2 program JAR",ActionOnFailure=CONTINUE,Jar=s3://neel-habiba/map2.jar,MainClass=MapReduce2,Args=[s3://neel-habiba/a7history,s3://neel-habiba/connections] \
--auto-terminate --log-uri s3://neel-habiba/logs --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging > ~/.aws/clusterID

aws emr describe-cluster --cluster-id  $(cat ~/.aws/clusterID| jq -r .ClusterId)| jq -r .Cluster.Status.State
echo 'Creating a cluster.. wait for the end message..';
i=0
while [[ "$(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq '.Cluster.Status.State')" != "\"TERMINATED\""  &&  "$(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq '.Cluster.Status.State')" != "\"TERMINATING\"" ]]
do
  echo -ne $(aws emr describe-cluster --cluster-id $(cat ~/.aws/clusterID| jq -r '.ClusterId') | jq -r '.Cluster.Status.State');
done
s3cmd get s3://neel-habiba/connections/* input/connections/

########## Run R script #########
Rscript Rscript.R input/





