package com.ifpi.analytics

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by IFPI on 22/07/2016.
  */
object TrackAnalytics extends App {

  import TrackAnalyticsOperations._

  lazy val inputPath = args(0)
  val sparkConfig = new SparkConf().setAppName("Track Analytics").set("region", args(1))

  implicit val sc = new SparkContext(sparkConfig)

  implicit val sqlContext = new SQLContext(sc)

  val tracksMetadata = sqlContext.read.json(inputPath)

  val tracks = tracksMetadata.trackData

  tracks.linksPerArtist().saveArtistsInDynamoDB
  tracks.linksPerTitle().saveTitlesInDynamoDB
}
