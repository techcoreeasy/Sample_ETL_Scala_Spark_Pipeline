package com.myhammer.net.conf

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark: SparkSession = {
    SparkSession.builder().master("local").appName("test spark session").getOrCreate()
  }
}
