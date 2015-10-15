This repository contains example code and resources for *Spark on Yarn* session.
Follow the below steps to clone code and setup your machine.


## Prerequisites

* Java
* Maven 3
* Hadoop 2.0+
* Yarn


## 2. Getting code

           git clone https://github.com/Shasidhar/spark-on-yarn


## 3. Build

        mvn clean install

### 4. Execution

We have to run the program by generating the jar and submitting to the hadoop cluster.

    
Follow the following steps to run the example.

#### Step 4.1 : Create jars folder in HDFS

This folder will hold the jar built in the build step. As we discussed earlier,
the jar containing application master has to be in HDFS in order to add as a local resource.


			hdfs dfs -mkdir /jars


#### Step 4.2 : Put the jar file in /jars

Copy the jar from your local file system to HDFS.


			hdfs dfs -put <jar-path> /jars


#### Step 4.3 : Run the code

Replace *jar-path* with absolute path to jar on you system. Also put appropriate values for namenode-host and namenode-port. The last parameter specifies number of containers.

			hadoop jar <jar-path>  com.madhukaraphatak.yarn.helloworld.Client hdfs://<namenode-host:namenode-port>/jars/yarn-helloworld-scala-1.0-SNAPSHOT.jar 1
