#!/bin/bash
rm -rf outputs
sbt clean test assembly
APP_JAR=$(ls target/scala-2.12/*.jar | head -n 1)

scala -J-Xmx2g \
  -classpath "${APP_JAR}" \
  ryan.widodo.MainApp \
  inputs/teleportData.csv \
  inputs/passengers.csv \
  outputs \
  2 \
  1997-01-01 \
  1997-05-01