import sbtassembly.AssemblyPlugin.autoImport._

lazy val root = (project in file("."))
  .settings(
    name         := "RandomCompanyHomework",
    scalaVersion := "2.12.10",
    version      := "1",
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
        .withIncludeDependency(true)
    }
  )

val sparkVersion = "2.4.8"

// provided = exclude from assembly.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "2.0.0"
libraryDependencies += "org.scalatest"        %% "scalatest" % "3.2.11" % Test
libraryDependencies += "org.scalamock"        %% "scalamock" % "5.1.0"  % Test
libraryDependencies += "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.2.1_1.3.0" % "test"
// To limit running Spark Test in parallel
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)
