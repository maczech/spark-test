package com.ifpi.analitycs

import java.util

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.Text

/**
  * Created by IFPI on 22/07/2016.
  */
object TrackAnalytics extends App {

  import TrackAnalyticsOperations._

  lazy val inputPath = args(0)
  val sparkConfig = new SparkConf().setAppName("Track Analytics")

  implicit val sc = new SparkContext(sparkConfig)

  implicit val sqlContext = new HiveContext(sc)

  val tracksMetadata = sqlContext.read.json(inputPath)

  val tracks = tracksMetadata.trackData

  tracks.linksPerArtist().saveArtistsInDynamoDB
  tracks.linksPerTitle().saveTitlesInDynamoDB
}
