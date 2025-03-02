#!/bin/bash
# Change the WORK_DIR as needed
WORK_DIR="$(pwd)"
INPUT_DIR="${WORK_DIR}/inputs"
OUTPUT_DIR="${WORK_DIR}/output_spark"
Teleport_DATA="${INPUT_DIR}/teleportData.csv"
PASSENGER_DATA="${INPUT_DIR}/passengers.csv"
SPARK_JAR="target/scala-2.12/RandomCompanyhomework_2.12-1.jar"

# Clean up
rm -rf $OUTPUT_DIR

sbt clean test package
SPARK_JAR=$(ls target/scala-2.12/*.jar | head -n 1)

SPARK_JOB_CLASS=ryan.widodo.spark.Question1Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${OUTPUT_DIR}"
spark-submit --master local[2] \
  --class $SPARK_JOB_CLASS \
  $SPARK_JAR \
  $SPARK_ARGUMENTS

SPARK_JOB_CLASS=ryan.widodo.spark.Question2Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${PASSENGER_DATA} ${OUTPUT_DIR}"
spark-submit --master local[2] \
  --class $SPARK_JOB_CLASS \
  $SPARK_JAR \
  $SPARK_ARGUMENTS

SPARK_JOB_CLASS=ryan.widodo.spark.Question3Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${OUTPUT_DIR}"
spark-submit --master local[2] \
  --class $SPARK_JOB_CLASS \
  $SPARK_JAR \
  $SPARK_ARGUMENTS

SPARK_JOB_CLASS=ryan.widodo.spark.Question4Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${OUTPUT_DIR}"
spark-submit --master local[10] \
  --class $SPARK_JOB_CLASS \
  $SPARK_JAR \
  $SPARK_ARGUMENTS

AT_LEAST_N_TIMES=2
DATE_FROM="1997-01-01"
DATE_TO="1997-05-01"
SPARK_JOB_CLASS=ryan.widodo.spark.Question5Spark
SPARK_ARGUMENTS="${Teleport_DATA} ${OUTPUT_DIR} ${AT_LEAST_N_TIMES} ${DATE_FROM} ${DATE_TO}"
spark-submit --master local[10] \
  --class $SPARK_JOB_CLASS \
  $SPARK_JAR \
  $SPARK_ARGUMENTS

echo "Done! Outputs are available at: ${OUTPUT_DIR}"