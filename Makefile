
all:
	(cd textsock && make)
	(cd client && sbt assembly && mv target/scala-*/Wordcount*jar Wordcount.jar)
	(cd server && sbt assembly && mv target/scala-*/server*jar server.jar)
start2:
	(sh start-cluster.sh 2)
start8:
	(sh start-cluster.sh 8)
my-mapreduce:
	(cp server/instances.txt client/)
	(cd client && java -jar Wordcount.jar s3://cs6240sp16/climate s3://<bucket-name>/output instances.txt)

stop:
	(sh stop-cluster.sh)

clean:
	(cd textsock && make clean)
	(cd client && rm -rf Wordcount.jar instances.txt topten.txt output target project/target project/project)
	(cd server && rm -rf server.jar instances.txt target project/target project/project)

.PHONY: all clean


