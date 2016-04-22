
all:
	(cd textsock && make)
	(cd client && sbt assembly && mv target/scala-*/client*jar client.jar)
	(cd server/hadoop && make)
	(cd server && sbt assembly && mv target/scala-*/server*jar server.jar)
	(cd server/wordcount && sbt assembly && mv target/scala-*/wordcount*jar wordcount.jar)
	(cd server/wordcount && cp wordcount.jar ../../server.jar)
start1:
	(bash start-cluster.sh 1)
start2:
	(bash start-cluster.sh 2)
start8:
	(sh start-cluster.sh 8)
my-mapreduce:
	(cd client && java -jar client.jar s3://ccshabiba/input s3://ccshabiba/output instances.txt WordCount)
stop:
	(sh stop-cluster.sh)
clean:
	(rm -rf server.jar)
	(cd textsock && make clean)
	(cd client && rm -rf client.jar server.jar instances.txt output target project/target project/project)
	(cd server && rm -rf server.jar instances.txt target project/target project/project)
	(cd server/hadoop && rm -rf target project/target project/project)
	(cd server/wordcount && rm -rf target project/target project/project wordcount.jar)
	(cd client/ && rm -rf allTempFiles)
.PHONY: all clean
	#Test


