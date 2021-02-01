package com.myhammer.net

import com.myhammer.net.conf.SparkConfiguration
import com.myhammer.net.etl.ETLProcess
import com.myhammer.net.processing.ServiceClassifierPipeline
import com.typesafe.config._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** A mainApp process that basically starting point as spark scala app. */

object mainApp extends App with SparkConfiguration {

  val serviceClassifierPreProcessing = new ServiceClassifierPreProcessing()

  class ServiceClassifierPreProcessing(spark: SparkSession = spark) {

    val serviceClassifierETL = buildServiceClassifierETLProcess.run()

    /** A buildServiceClassifierETLProcess is the process that extracts data of type IN, transform it to OUT and load it to s3. */

    private def buildServiceClassifierETLProcess = {

      new ETLProcess[DataFrame, DataFrame, Unit] with ServiceClassifierPipeline {

        val config = ConfigFactory.load("serviceclassifier.conf")

        val INPUT_DATA_PATH = config.getString("S3_INPUT_DATA_PATH")

        val S3_OUTPUT_DATA_PATH = config.getString("S3_OUTPUT_DATA_PATH")

        override def extract(): DataFrame = spark.read.parquet(INPUT_DATA_PATH)

        override def load(out: DataFrame): Unit = out.write.mode("overwrite").text(S3_OUTPUT_DATA_PATH)

        override def _spark: SparkSession = spark

      }
    }
  }
}
