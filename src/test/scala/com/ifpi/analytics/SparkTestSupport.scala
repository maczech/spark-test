package com.ifpi.analytics

import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.SparkContext

trait SparkTestSupport {

  def withSparkContext( test: SparkContext => Unit  ): Unit = {

    val sc = new SparkContext("local[1]", RandomStringUtils.randomAlphabetic(10))

    try {
      test(sc)
    } finally {
      sc.stop()
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.master.port")
    }
  }

}
