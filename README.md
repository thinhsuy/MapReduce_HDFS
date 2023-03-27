# MapReduce with Hadoop distributed-file System
## Introduction
- On standard hardware, the distributed file system HDFS manages enormous data sets. A single Apache Hadoop cluster can be scaled up to hundreds or even thousands of nodes using this technique. One of Apache Hadoop's key parts, along with MapReduce and YARN, is HDFS.
- Accessing large amounts of data housed in the Hadoop File System requires the usage of the MapReduce programming paradigm or pattern. (HDFS). It is essential to the operation of the Hadoop framework and a core component.
By dividing petabytes of data into smaller chunks and processing them in parallel on Hadoop commodity servers, MapReduce makes concurrent processing easier. In the end, it collects all the information from several servers and gives the application a consolidated output.

![MapReduce1-3](https://user-images.githubusercontent.com/81562297/227573358-4dc596e0-2a54-499f-9d38-84fe161685cf.jpg)

# Requirements
1. Your OS's environment already contained [JAVA](https://www.oracle.com/java/technologies/downloads) (If your OS was Linux, the installation already included in second step).
2. Be sure that [Hadoop](https://www.geeksforgeeks.org/how-to-install-hadoop-in-linux/) already installed in your Operating System or [Virtual Machine](https://kb.vmware.com/s/article/2053973).
2. Starting all neccessary clusters and reviewing by JPS. (With Hadoop version 2.0 and later, localhost moved to http://localhost:9870 instead of http://localhost:50070).
4. First test with [WordCount Execution](https://www.youtube.com/watch?v=6sK3LDY7Pp4&ab_channel=MohammedSheeha).

![download](https://user-images.githubusercontent.com/81562297/227574049-cb044bbe-77df-47eb-861c-2c6c09eddcc3.png)

# Operation
- First and foremost, it is crucial to configure Hadoop Classpath and Hadoop Home for the system. After assigning, always check the path.
```
>> export HADOOP_CLASSPATH=$(hadoop classpath)
>> echo $HADOOP_CLASSPATH
```
- Second, for each solution you want to use, let's build the principal folder and its input using hadoop remote commands.
```
>> hadoop fs -mkdir /WordCount
>> hadoop fs -mkdir /WordCount/Input
```
- Next, `-put` the `input.txt` file into the `input_data` folder on the Hadoop server. If additional input files are discovered, simply follow these steps:
```
>> hadoop fs -put '/input_data/input.txt' '/WordCount/Input'
```
- Before combining the.java file into multiple classes files, it should be reviewed:
```
>> javac -classpath $(hadoop classpath) -d 'java_classes' 'WordCount.java'
```
- Continue putting the extracted multi-classes files into compilation .jar
```
>> jar -cvf WordCount.jar -C java_classes/ .
```
- After generating .jar file, conduct the MapReduce operation on Hadoop Server
```
>> hadoop jar 'WordCount.jar' WordCount '/WordCount/Input' '/WordCount/Output'
```
- From here, the running program on the cluster's MapReduce process has already finished, and the results can be examined by:
```
>> hdfs dfs -cat /WordCount/Output/*
```

# References
https://www.java.com/en/
https://www.oracle.com/
https://onlineitguru.com/blogger/explain-hadoop-architecture-and-its-main-components
https://www.geeksforgeeks.org/how-to-install-hadoop-in-linux/
https://www.youtube.com/watch?v=6sK3LDY7Pp4&ab_channel=MohammedSheeha
