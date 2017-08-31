import sbt._

object Dependencies {

  val akkaHttpVersion = "2.4.11"

  val sparkV = "2.1.1"

  lazy val framelessV = "0.3.0"

  val sparkCore =               "org.apache.spark" %% "spark-core" % sparkV
  val sparkStreaming =          "org.apache.spark" %% "spark-streaming" % sparkV
  val sparkStreamingKafka =     "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkV
  val sparkSql =                "org.apache.spark" %% "spark-sql" % sparkV
  val kafka =                   "org.apache.kafka" %% "kafka" % "0.8.2.2"
  val akkaHttp =                "com.typesafe.akka" %% "akka-http-experimental" % akkaHttpVersion
  val lift =                    "net.liftweb" %% "lift-json" % "2.6"
  val twitterStream =           "org.twitter4j" % "twitter4j-stream" % "4.0.4"
  val sparkCassandraConnect =   "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"
  val cassandraDriver =         "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"
  val logback =                 "ch.qos.logback" % "logback-classic" % "1.1.7"
  val akkaHttpJson =            "de.heikoseeberger"          %% "akka-http-json4s"               % "1.7.0"
  val json4s =                  "org.json4s"    %% "json4s-native"      %  "3.3.0"
  val jansi =                   "org.fusesource.jansi"       %  "jansi"                          % "1.12"
  val cats = "org.typelevel" %% "cats" % "0.9.0"
  val framelessCats = "org.typelevel" %% "frameless-cats"      % framelessV
  val framelessDS = "org.typelevel" %% "frameless-dataset"   % framelessV
}