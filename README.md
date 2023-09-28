# MapReduce with Hadoop distributed-file System (HDFS)
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

# Methodology
In this situation, WordCount would be the precise illustration of how Hadoop operates to solve all problems. The entire procedure is carried out in four stages: dividing, mapping, shuffle, and reduction. Let's use a MapReduce example to better grasp this MapReduce explanation. Consider you have following input data for your MapReduce in Big data Program.

![061114_0930_Introductio1](https://user-images.githubusercontent.com/81562297/228138242-559df943-a9f0-4112-96e8-ea4fd45be1ad.png)

## Input Splits
Input splits, or chunks of the input that are eaten by a single map, are fixed-size parts of the input that are used in a MapReduce operation in a Big Data job.

## Mapping
This is the initial stage of the map-reduce program's execution. Data from each split is provided to a mapping function during this phase, which generates output values. In our example, the mapping phase's task is to count how many times each word appears in the input divides (additional information about the input split is provided below) and to create a list in the format of "word, frequency."

## Shuffling
The result from the mapping phase is used in this phase. Its duty is to compile the pertinent results from the Mapping step. The same terms and their corresponding frequencies are combined in our case.

## Reducing
In this phase, output values from the Shuffling phase are aggregated. This phase combines values from Shuffling phase and returns a single output value. This stage, in a nutshell, describes the entire dataset.

# Organization Works
Hadoop divides the job into tasks. There are two types of tasks:
- Map tasks (Splits & Mapping)
- Reduce tasks (Shuffling, Reducing)

Both the Map and Reduce task execution processes are under the direction of two different sorts of entities known as a
- Jobtracker: Acts like a master (responsible for complete execution of submitted job)
- Multiple Task Trackers: Acts like slaves, each of them performing the job

![061114_0930_Introductio2](https://user-images.githubusercontent.com/81562297/228139044-55e53959-f1e8-482b-a99e-358eb1cf1c09.png)

- A job is split up into several tasks, each of which is executed on a different data node in the cluster.
- The job tracker is in charge of organizing the activity by setting up jobs to run on various data nodes.
- The task tracker, which is present on every data node carrying out a job component, is therefore responsible for monitoring the execution of each individual task.
- Sending the job tracker the progress report is the task tracker's duty. 
- Additionally, the task tracker sends a "heartbeat" signal to the job tracker on a regular basis to let him know how the system is doing. 
- The total progress of each job is thus monitored by the job tracker. 
- The job tracker has the ability to reschedule a failed task to another task tracker.


# References
https://www.java.com/en/
https://www.oracle.com/
https://onlineitguru.com/blogger/explain-hadoop-architecture-and-its-main-components
https://www.geeksforgeeks.org/how-to-install-hadoop-in-linux/
https://www.youtube.com/watch?v=6sK3LDY7Pp4&ab_channel=MohammedSheeha
https://www.guru99.com/introduction-to-mapreduce.html
