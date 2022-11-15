ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "Ass2mongodb"
  )
libraryDependencies ++=Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",

)

// https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver
libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.12.10"

// https://mvnrepository.com/artifact/com.github.nscala-time/nscala-time
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.32.0"
