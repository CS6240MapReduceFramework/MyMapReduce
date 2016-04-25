Set up: 

1. Download your <Keypair>.pem in AWS EC2
2. Update your AWS AWSAccessKeyId and AWSSecretKey in config.properties file
3. Open start-clushter.sh and update your security-group and keyPair values
4. Open my-mapreduce.sh and update your keyPair value
5. All map-reduce programs are in Hadoop/src/server folder

Instruction to run in distributed mode:

1. ./start-cluster.sh <No.of instances> distributed
2. ./my-mapreduce.sh wordcount s3://<input> s3://<output> distributed
3. ./stop-cluster.sh


Instruction to run in pseudo mode:

1. ./start-cluster.sh <No.of instances> pseudo
2. ./my-mapreduce.sh wordcount s3://<input> s3://<output> pseudo
3. ./stop-cluster.sh
