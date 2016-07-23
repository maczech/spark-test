package com.ifpi.analytics

import org.scalatest.{Matchers, WordSpec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class TrackAnalyticsOperationsSpec extends WordSpec with Matchers with SparkTestSupport {

  import TrackAnalyticsOperations._

  val schema = StructType(StructField("link", StringType, true)
    :: StructField("title", StructType(StructField("titlename", StringType, true) :: Nil))
    :: StructField("artist", StructType(StructField("artist", StringType, true) :: Nil))
    :: Nil)

  "track analytics operations" should {
    "extract track data from full dataset" in withSparkContext { implicit sc =>
      implicit val sqlContext = new SQLContext(sc)

      val inputRecords =
        sc.parallelize(Seq(Row("sample link", Row("sample artist"), Row("sample title"))))

      val inputData = sqlContext.createDataFrame(inputRecords, schema)

      val result = inputData.trackData.collect()(0)
      result should equal(new TrackData("sample link", "sample artist", "sample title"))
    }

    "skip record when link is null" in withSparkContext { implicit sc =>
      implicit val sqlContext = new SQLContext(sc)

      val inputRecords =
        sc.parallelize(Seq(Row(null, Row("sample artist"), Row("sample title"))))

      val inputData = sqlContext.createDataFrame(inputRecords, schema)

      val result = inputData.trackData.collect()
      result should have size 0
    }

    "count links per artist" in withSparkContext { implicit sc =>
      val inputRecords = sc.parallelize(Seq(
        new TrackData("link 1", "track 1", "artist 1"),
        new TrackData("link 2", "track 2", "artist 1"),
        new TrackData("link 3", "track 3", "artist 2")))

      val result = inputRecords.linksPerArtist.collect()
      result should contain theSameElementsAs Seq(new ArtistsCount("artist 1", 2), new ArtistsCount("artist 2", 1))
    }

    "count links per title" in withSparkContext { implicit sc =>
      val inputRecords = sc.parallelize(Seq(
        new TrackData("link 1", "track 1", "artist 1"),
        new TrackData("link 2", "track 1", "artist 1"),
        new TrackData("link 3", "track 2", "artist 2")))

      val result = inputRecords.linksPerTitle.collect()
      result should contain theSameElementsAs Seq(new ArtistsCount("track 1", 2), new ArtistsCount("track 2", 1))
    }
  }
}
