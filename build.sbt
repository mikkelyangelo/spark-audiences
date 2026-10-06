ThisBuild / scalaVersion := "2.12.20"
ThisBuild / version := "0.1.0"

lazy val sparkVersion = "3.5.3"

// Spark 3.5 on Java 17 needs these opens, spark-submit adds them itself
lazy val java17Opens = Seq(
  "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
  "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
  "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"
).map(p => s"--add-opens=java.base/$p=ALL-UNNAMED")

lazy val root = (project in file("."))
  .settings(
    name := "spark-audiences",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint"),
    fork := true,
    javaOptions ++= java17Opens :+ "-Xmx2g",
    // `sbt run` keeps Provided Spark on the classpath and runs locally
    Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
    Compile / run / javaOptions += "-Dspark.master=local[*]",
    Test / parallelExecution := false
  )
