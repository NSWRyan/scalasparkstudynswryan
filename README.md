# Disclaimer
* This is a Scala/Spark HW for a company that I interviewed with last year.
* All of the questions have been reworded such that Google search should not help cheaters.
* In respect to the company, the example data has been randomized.
* This is for personal data reference for my self study.

# RandomCompany Homework
* Solutions by NSWRyan.
* I did it 2 times.
  * 1st time with Scala only.
  * 2nd time with Spark Scala.

## Impression
* This is a fun homework.
* My first time dealing with Scala this deep.
* I normally do it in Python Panda or PySpark.
* I learned a lot on Scala, so thank you for the homework.
* P.S. My solutions might not be perfect as I have limited experience with Spark Scala.
* I do know how to deploy Spark cluster on k8s or Yarn.
* All done on Windows using WSL with Ubuntu 22.04.2 LTS.

## Table of Contents

1. [Setup (WSL)](#setup-wsl)
    - [Install Oracle JDK 1.8](#install-oracle-jdk-18)
    - [Install sbt and scala using coursier](#install-sbt-and-scala-using-coursier)
    - [Install Spark 2.4.8](#install-spark-248)
    - [Install Hadoop 3.4.0](#install-hadoop-340)
    - [Example snippet of the modified .bashrc](#example-snippet-of-the-modified-bashrc)
    - [Testing installation](#testing-installation)
    - [Clean up](#clean-up)

2. [Scala homework answers](#scala-homework-answers)
    - [Building Homework jar](#building-homework-jar)
    - [Compiling Scala Homework jar (Fat Jar)](#compiling-scala-homework-jar-fat-jar)
    - [Running the jar (from project's root)](#running-the-jar-from-projects-root)

3. [Scala Spark version](#scala-spark-version)
    - [Compiling the code](#compiling-the-code)
    - [Base bash variables](#base-bash-variables-feel-free-to-replace-the-path-with-your-path)
    - [Submitting jar to local cluster and run answers](#submitting-jar-to-local-cluster-and-run-answers)
        - [Question 1](#question1)
        - [Question 2](#question2)
        - [Question 3](#question3)
        - [Question 4](#question4)
        - [Question 5](#question5)
    - [All in one script](#all-in-one-script)

4. [Unit Test](#unit-test)

## Code structure
```bash
src
├── main
│   └── scala
│       └── ryan
│           └── widodo
│               ├── MainApp.scala
│               ├── Utils.scala
│               ├── answers
│               │   ├── Question1.scala
│               │   ├── Question2.scala
│               │   ├── Question3.scala
│               │   ├── Question3Helper.scala
│               │   ├── Question4.scala
│               │   └── Question5.scala
│               ├── dao
│               │   ├── Teleport.scala
│               │   └── Passenger.scala
│               ├── readers
│               │   ├── teleportDataReader.scala
│               │   └── PassengerDataReader.scala
│               └── spark
│                   ├── Question1Spark.scala
│                   ├── Question2Spark.scala
│                   ├── Question3Spark.scala
│                   ├── Question4Spark.scala
│                   └── Question5Spark.scala
└── test
    └── scala
        └── ryan
            └── widodo
                ├── MainAppTest.scala
                ├── NoExitSecurityManager.scala
                ├── UtilsTest.scala
                ├── answers
                │   ├── Question1Test.scala
                │   ├── Question2Test.scala
                │   ├── Question3HelperTest.scala
                │   ├── Question3Test.scala
                │   ├── Question4Test.scala
                │   └── Question5Test.scala
                ├── readers
                │   ├── TeleportDataReaderTest.scala
                │   └── PassengerDataReaderTest.scala
                └── spark
                    ├── Question1SparkTest.scala
                    ├── Question2SparkTest.scala
                    ├── Question3SparkTest.scala
                    ├── Question4SparkTest.scala
                    └── Question5SparkTest.scala

```

## Setup (WSL)

### Install Oracle JDK 1.8
```bash
# Download first:
https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-
2133151.html

# Extract the jdk
tar -xzvf jdk-8u421-linux-x64.tar.gz
mv jdk1.8.0_421 /usr/lib/jvm/jdk1.8.0_421

# Install OracleJDk
sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk1.8.0_421/bin/java 2

# Set OracleJDK as the default (pick /usr/lib/jvm/jdk1.8.0_421/bin/java)
sudo update-alternatives --config java

echo 'export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_421' >> ~/.bashrc
rm -rf jdk-8u421-linux-x64.tar.gz
```

### Install sbt and scala using coursier
```bash
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup
# Check sbt
sbt -version
cs install scala:2.12.10
# Check scala version
scala -version
# Scala code runner version 2.12.10 -- Copyright 2002-2019, LAMP/EPFL and Lightbend, Inc.
```

### Install Hadoop 3.4.0
```bash
# Download Hadoop release.
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz ;
# Extract it
tar -xzvf hadoop-3.4.0.tar.gz ;
sudo mv hadoop-3.4.0 /opt/hadoop-3.4.0 ;
sudo ln -s /opt/hadoop-3.4.0 /opt/hadoop ;

# Add Hadoop to path
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc
echo 'export SPARK_DIST_CLASSPATH=$(hadoop classpath)' >> ~/.bashrc
# Restart shell or source .bashrc
source ~/.bashrc
```

### Install spark 2.4.8
```bash
cd /tmp/
# Download spark 2.4.8 with scala 2.12.
# Note that we need to dl hadoop separately 
# as it requires some library in Hadoop.
wget https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-without-hadoop-scala-2.12.tgz
# Extract it.
tar -xzvf spark-2.4.8-bin-without-hadoop-scala-2.12.tgz
# Move to /opt/spark
sudo mv spark-2.4.8-bin-without-hadoop-scala-2.12 /opt/spark

# Add spark to ~/.bashrc
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc

# Test spark
spark-shell

# ctrl+c to quit shell
```

#### Example snippet of the modified .bashrc
```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_421
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
```

### Testing installation
```bash
# Hadoop
hadoop version
# spark
spark-shell
```

### Clean up
```bash
rm -rf /tmp/spark-2.4.8-bin-without-hadoop-scala-2.12.tgz
rm -rf /tmp/hadoop-3.4.0.tar.gz
```

## Scala homework answers
### Building Homework jar
```bash
# cd to project root dir. first.
sbt test package
```

### Compiling Scala Homework jar (Fat Jar)
```bash
# cd to project root dir. first.
# We use assembly here to package
# necessary libraries inside the jar 
# to avoid specifying the library classpath.
# If the library is used by many other jars, 
# its better to use classpath. 
sbt test assembly
```
* Note that the fat/uber jar excludes Scala library.
* So it needs to be run with scala command.

### Running the jar (from project's root)
```bash
# -J-Xmx2g is there so that the code run 
# a bit faster to reduce Jvm gc call.
scala -J-Xmx2g \
 -classpath target/scala-2.12/RandomCompanyHomework-assembly-1.jar \
 ryan.widodo.MainApp \
 inputs/teleportData.csv \
 inputs/passengers.csv \
 outputs
 
 # With extra parameters
 scala -J-Xmx2g \
 -classpath target/scala-2.12/RandomCompanyHomework-assembly-1.jar \
 ryan.widodo.MainApp \
 inputs/teleportData.csv \
 inputs/passengers.csv \
 outputs \
 2 \
 1997-01-01 \
 1997-05-01
 
```
## Scala Spark version
## Compiling the code.
```bash
# Package is enough here as we don't use 
# special library to read csv like in 
# the pure Scala code.
sbt test package
```

### Base bash variables (Feel free to replace the path with your path)
```bash
WORK_DIR="$(pwd)"
INPUT_DIR="${WORK_DIR}/inputs"
OUTPUT_DIR="${WORK_DIR}/output_spark"
Teleport_DATA="${INPUT_DIR}/teleportData.csv"
PASSENGER_DATA="${INPUT_DIR}/passengers.csv"
SPARK_JAR="target/scala-2.12//randomcompanyhomework_2.12-1.jar"
```

### Submitting jar to local cluster and run answers
#### Question1
```bash
SPARK_JOB_CLASS=ryan.widodo.spark.Question1Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${OUTPUT_DIR}"
spark-submit --master local[2] \
 --class $SPARK_JOB_CLASS \
 $SPARK_JAR \
 $SPARK_ARGUMENTS
```

#### Question2
```bash
SPARK_JOB_CLASS=ryan.widodo.spark.Question2Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${PASSENGER_DATA} ${OUTPUT_DIR}"
spark-submit --master local[2] \
 --class $SPARK_JOB_CLASS \
 $SPARK_JAR \
 $SPARK_ARGUMENTS
```

#### Question3
```bash
SPARK_JOB_CLASS=ryan.widodo.spark.Question3Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${OUTPUT_DIR}"
spark-submit --master local[2] \
 --class $SPARK_JOB_CLASS \
 $SPARK_JAR \
 $SPARK_ARGUMENTS
```

#### Question4
```bash
SPARK_JOB_CLASS=ryan.widodo.spark.Question4Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${OUTPUT_DIR}"
spark-submit --master local[10] \
 --class $SPARK_JOB_CLASS \
 $SPARK_JAR \
 $SPARK_ARGUMENTS
```

#### Question5
```bash
rm -rf rm output_spark/Question5Spark.csv
AT_LEAST_N_TIMES=2
DATE_FROM="1997-01-01"
DATE_TO="1997-05-01"
SPARK_JOB_CLASS=ryan.widodo.spark.Question5Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${OUTPUT_DIR} ${AT_LEAST_N_TIMES} ${DATE_FROM} ${DATE_TO}"
spark-submit --master local[10] \
 --class $SPARK_JOB_CLASS \
 $SPARK_JAR \
 $SPARK_ARGUMENTS
```
### All in one script
* Available as run_spark.sh.
* Available as run_scala.sh.

# Unit Test
* Unit tests are handled using ScalaTest.
```bash
# Run all:
sbt test
# Run one test class
TEST_CLASS="ryan.widodo.spark.Question4SparkTest"
sbt "test:testOnly TEST_CLASS"
```
* I tried to cover all edge cases, but there might be some that I missed.