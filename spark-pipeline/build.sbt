name := "spark-pipeline"
version := "1.0.0"
scalaVersion := "3.3.3"

Test / fork := true
run / fork := true
Test / parallelExecution := false

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

conflictWarning := ConflictWarning.disable

javaOptions ++= Seq(
  "-Dhadoop.home.dir=d:/proys_2/fraud-pattern-detector/winutils",
  "-Djava.library.path=d:/proys_2/fraud-pattern-detector/winutils/bin",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
