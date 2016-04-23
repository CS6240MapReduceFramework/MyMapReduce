
all:
	(cd textsock && make)
	(cd client && sbt assembly && mv target/scala-*/client*jar client.jar)
	(cd server/wordcount && sbt assembly && mv target/scala-*/wordcount*jar ../../server.jar)

1start:
	(bash start-cluster.sh 1 distributed)
2start:
	(bash start-cluster.sh 2 distributed)
8start:
	(bash start-cluster.sh 8 distributed)
pseudostart2:
	(bash start-cluster.sh 2 pseudo)
my-mapreduce:
	(cd client && java -jar client.jar s3://kvs3test/input s3://kvs3test/output instances.txt WordCount)
stop:
	(bash stop-cluster.sh)
clean:
	(rm -rf server.jar)
	(cd textsock && make clean)
	(cd client && rm -rf client.jar server.jar output target project/target project/project)
	(cd server && rm -rf server.jar instances.txt target project/target project/project)
	(cd server/hadoop && rm -rf target project/target project/project)
	(cd server/wordcount && rm -rf target project/target project/project wordcount.jar)
	(cd client/ && rm -rf allTempFiles)
	(cd client/ && rm -rf FinalOutput)
	(cd client && rm instances.txt)
.PHONY: all clean
	#Test


