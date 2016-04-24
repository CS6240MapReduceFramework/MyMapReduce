format: hstop
	hdfs namenode -format

hstart: format
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop: clean
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh

neel: hstart
	hadoop fs -mkdir -p input
	hadoop fs -put all/* input

run: neel
	gradle build
	cp build/libs/*.jar Main.jar
	hadoop jar Main.jar Main

clean:
	rm -rf build forests/* Main.jar

emr:
	./emr.sh
