name := "DenormalizeTables"
version := "0.1"
scalaVersion := "2.12.15" // Используйте версию Scala, совместимую с вашим Spark

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "mysql" % "mysql-connector-java" % "8.0.30" // Драйвер для MySQL
)
