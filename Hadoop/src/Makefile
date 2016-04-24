run:
	bash start-cluster.sh 2 distributed
	bash my-mapreduce.sh ${ARGS} s3://ccshabiba/input s3://ccshabiba/output distributed
	bash stop-cluster.sh

all:
	(cd textsock && make)
	(cd client && sbt assembly && mv target/scala-*/client*jar client.jar)
	(cd server/hadoop && make)

clean:
	(rm -rf server.jar instances.txt server_log.txt)
	(cd textsock && make clean)
	(cd client && rm -rf client.jar instances.txt server.jar output target project/target project/project)
	(cd server && rm -rf server.jar instances.txt target project/target project/project)
	(cd server/hadoop && rm -rf target project/target project/project)
	(cd server/${ARGS} && rm -rf target project/target project/project wordcount.jar)
	(cd client/ && rm -rf allTempFiles)
	(cd client/ && rm -rf FinalOutput)

.PHONY: all clean
	#Test


