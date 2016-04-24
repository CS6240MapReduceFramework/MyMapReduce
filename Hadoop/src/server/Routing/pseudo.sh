## Author: Habiba Neelesh
#!/bin/bash
echo "Running Script!"

## EMR - Hadoop
mr-jobhistory-daemon.sh stop historyserver
stop-yarn.sh
stop-dfs.sh
start-dfs.sh
start-yarn.sh
mr-jobhistory-daemon.sh start historyserver

#transfer input folders
hadoop fs -put a7history /
hadoop fs -put a7test /

#remove output folders in hadoop fs
hadoop fs -rmr /history
hadoop fs -rmr /connections

#create jars
hadoop com.sun.tools.javac.Main Main.java
jar cf main.jar Main*.class
hadoop com.sun.tools.javac.Main MapReduce2.java
jar cf map2.jar MapReduce2*.class

#Run programs on hadoop
hadoop jar main.jar Main /a7history /history
hadoop jar map2.jar MapReduce2 /a7test /connections

#copy outputs to local input folder
hadoop fs -get /history input/
hadoop fs -get /connections input/

########## Run R script #########
Rscript Rscript.R input/





