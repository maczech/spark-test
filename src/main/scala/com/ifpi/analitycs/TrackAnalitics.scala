package com.ifpi.analitycs

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by IFPI on 22/07/2016.
  */
object TrackAnalytics extends App{
 lazy val inputPath = args(0)
 lazy val outputPath = args(1)
  val sparkConfig = new SparkConf().setAppName("Track Analytics")

  val sc = new SparkContext(sparkConfig)

  val sqlContext = new HiveContext(sc)

  val tracksMetadata = sqlContext.read.json(inputPath)

  tracksMetadata.registerTempTable("tracks")

  val trackDetails = sqlContext.sql("select link, title.titlename, artist.artist from tracks")
    .rdd.map(row => (row(0),row(1),row(2))).filter(_._1 != null).cache

  val linksPerArtist = trackDetails.keyBy(_._3).groupByKey().map{ case (artist, records) =>
   (artist, records.size)
  }

 val linksPerTitle = trackDetails.keyBy(_._2).groupByKey().map{ case (title, records) =>
  (title, records.size)
 }

 linksPerArtist.saveAsTextFile(s"${outputPath}/artists")
 linksPerTitle.saveAsTextFile(s"${outputPath}/title")
}
