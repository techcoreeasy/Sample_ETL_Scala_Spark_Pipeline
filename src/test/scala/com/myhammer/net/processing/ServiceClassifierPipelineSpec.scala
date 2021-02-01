package com.myhammer.net.processing

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.myhammer.net.conf.SparkSessionTestWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class ServiceClassifierPipelineSpec extends FlatSpec with SparkSessionTestWrapper with DataFrameComparer with ServiceClassifierPipeline {

  implicit val sp: SparkSession = spark
  override def _spark: SparkSession = spark
  behavior of "ServiceClassifierPipelineSpec"

  it should "compute the pipeline of pre-processing of service classifier pre-processing" in {

    val inputPath = getClass.getResource("/input_data/service_classifier_pipeline_input.txt").getPath
    val outputPath = getClass.getResource("/expected_data/service_classifier_pipeline_expected.txt").getPath

    val inputDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(inputPath)

    val resultDF = transform(inputDF)
    val expectedDF = sp.read.option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(outputPath)

    assertSmallDataFrameEquality(resultDF, expectedDF)

  }

}
