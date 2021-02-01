package com.myhammer.net.conf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkConfiguration {

  /** A SparkConfiguration trait which create SparkSession */

  val accessKeyId: String = System.getenv("AWS_ACCESS_KEY_ID")
  val secretAccessKey: String = System.getenv("AWS_SECRET_ACCESS_KEY")

  val conf: SparkConf = new SparkConf()
    .set("fs.s3a.awsAccessKeyId", accessKeyId)
    .set("fs.s3a.awsSecretAccessKey", secretAccessKey)
    .set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
    .set("fs.s3a.connection.ssl.enabled", "true")

  /* Creating Spark Context */
  val spark = SparkSession
    .builder
    .config(conf)
    .master("local")
    .appName("service-text-classification-spark-app")
    .getOrCreate()

}
