
all:
	(cd textsock && make)
	(cd client && sbt assembly && mv target/scala-*/client*jar client.jar)
	(cd server/hadoop && make)
	# (cd server && sbt assembly && mv target/scala-*/server*jar server.jar)
	(cd server/wordcount && sbt assembly && mv target/scala-*/wordcount*jar wordcount.jar)
	(cd server/wordcount && cp wordcount.jar ../../server.jar)
start2:
	(sh start-cluster.sh 2)
start8:
	(sh start-cluster.sh 8)
my-mapreduce:
	(cd client && java -jar client.jar s3://kvs3test/input s3://kvs3test/output instances.txt WordCount)

stop:
	(sh stop-cluster.sh)

clean:
	(rm -rf server.jar)
	(cd textsock && make clean)
	(cd client && rm -rf client.jar server.jar instances.txt output target project/target project/project)
	(cd server && rm -rf server.jar instances.txt target project/target project/project)
	(cd server/hadoop && rm -rf target project/target project/project)
	(cd server/wordcount && rm -rf target project/target project/project wordcount.jar)
.PHONY: all clean
	#Test


