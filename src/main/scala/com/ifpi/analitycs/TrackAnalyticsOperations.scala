package com.ifpi.analitycs

import java.util

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext



object TrackAnalyticsOperations {
  type Link = String
  type Artist = String
  type Title = String
  type Count = Int
  type TrackData = (Link, Artist, Title)
  type ArtistsCount = (Artist, Count)
  type TitlesCount = (Title, Count)

  implicit class pimpedTrackAnalyticsDataFrame(val self: DataFrame) extends AnyVal {

    def trackData(implicit sqlContext: HiveContext): RDD[TrackData] = {
      self.registerTempTable("tracks")

      sqlContext.sql("select link, title.titlename, artist.artist from tracks")
        .rdd.filter(_(0) != null).map(row => new TrackData(row(0).toString, row(1).toString, row(2).toString)).cache
    }

  }

  implicit class pimpedTrackAnalyticsRDD(val self: RDD[TrackData]) extends AnyVal {

    def linksPerArtist(): RDD[ArtistsCount] = {
      self.keyBy(_._3).groupByKey().map { case (artist, records) =>
        (artist, records.size)
      }
    }

    def linksPerTitle(): RDD[TitlesCount] = {
      self.keyBy(_._2).groupByKey().map { case (title, records) =>
        (title, records.size)
      }
    }
  }

  implicit class pimpedCountersRDD(val self: RDD[(String, Count)]) extends AnyVal {

    def saveArtistsInDynamoDB(implicit sc:SparkContext): Unit = {
      TrackAnalyticsOperations.saveInDynamoDB(self, "links_per_artist", "artist")
    }

    def saveTitlesInDynamoDB(implicit sc:SparkContext): Unit = {
      TrackAnalyticsOperations.saveInDynamoDB(self, "links_per_title", "title")
    }
  }


  def saveInDynamoDB(rdd:RDD[(String, Int)],tableName:String, partitionKey: String)(implicit sc:SparkContext): Unit = {
    var ddbConf = new JobConf(sc.hadoopConfiguration)
    ddbConf.set("dynamodb.output.tableName", tableName)
    ddbConf.set("dynamodb.throughput.write.percent", "0.5")
    ddbConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
    ddbConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    ddbConf.set("dynamodb.servicename", "dynamodb")
    ddbConf.set("dynamodb.endpoint", "dynamodb.eu-west-1.amazonaws.com")
    ddbConf.set("dynamodb.regionid", "eu-west-1")

    var ddbInsertFormattedRDD = rdd.map(row => {
      var ddbMap = new util.HashMap[String, AttributeValue]()
      var partitionKeyValue = new AttributeValue()
      partitionKeyValue.setS(row._1.toString)
      ddbMap.put(partitionKey, partitionKeyValue)
      var countValue = new AttributeValue()
      countValue.setN(row._2.toString)
      ddbMap.put("count", countValue)
      var item = new DynamoDBItemWritable()
      item.setItem(ddbMap)
      (new Text(""), item)
    }
    )
    ddbInsertFormattedRDD.saveAsHadoopDataset(ddbConf)
  }

}