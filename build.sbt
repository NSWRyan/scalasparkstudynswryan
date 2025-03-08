import sbtassembly.AssemblyPlugin.autoImport._

lazy val root = (project in file("."))
  .settings(
    name := "RandomCompanyHomework",
    scalaVersion := "2.13.16",
    version := "1",
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
        .withIncludeDependency(true)
    }
  )

val sparkVersion = "3.3.2"

// provided = exclude from assembly.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "2.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
libraryDependencies += "org.scalamock" %% "scalamock" % "6.2.0" % Test
libraryDependencies += "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.3.2_2.0.1" % "test"
// To limit running Spark Test in parallel
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)
