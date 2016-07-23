name := "spark_test"

version := "1.0"

scalaVersion := "2.10.5"
val sparkVersion ="1.6.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.20" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.3.2" % "test"
)


mainClass in assembly := Some("")

test in assembly := {}

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
scalacOptions += "-target:jvm-1.7"


assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

assemblyJarName in assembly := s"${name.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)               => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html"       => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class"      => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".jnilib" 	   => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".dll" 	     => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".css" 	     => MergeStrategy.discard
  case "application.conf"                                  => MergeStrategy.concat
  case "unwanted.txt"                                      => MergeStrategy.discard
  case "plugin.xml"                                        => MergeStrategy.discard
  case "parquet.thrift"                                    => MergeStrategy.discard


  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
    