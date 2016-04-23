
# make all ARG=wordcount
all:
	(cd textsock && make)
	(cd client && sbt assembly && mv target/scala-*/client*jar client.jar)
	(cd server/hadoop && make)
	(cd server && sbt assembly && mv target/scala-*/server*jar server.jar)
	(cd server/$(ARG) && sbt assembly && mv target/scala-*/$(ARG)*jar $(ARG).jar)
	(cd server/$(ARG) && cp $(ARG).jar ../../server.jar)

# make all-local ARG=wordcount
all-local:
	(cd server/hadoop && make)
	(cd server && sbt assembly && mv target/scala-*/server*jar server.jar)
	(cd server/$(ARG) && sbt assembly && mv target/scala-*/$(ARG)*jar $(ARG).jar)

	# make start ARG=2
	# make start ARG=4
start:
	(bash start-cluster.sh $(ARG))
	# make my-mapreduce ARG=wordcount
	# make my-mapreduce ARG=wordmedian
my-mapreduce:
	(make all ARG=$(ARG))
	(bash copy-jar-to-server.sh)
	(cd client && java -jar client.jar s3://kvs3test/input s3://kvs3test/output instances.txt WordCount)

# make my-mapreduce-local ARG=wordcount
my-mapreduce-local:
	(make all-local ARG=$(ARG))
	(cd server/$(ARG) && )
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
.PHONY: all clean
	#Test


