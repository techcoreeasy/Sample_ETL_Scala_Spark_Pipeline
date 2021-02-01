package com.myhammer.net.stages

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.myhammer.net.conf.SparkSessionTestWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class RequiredColProcessingSpec extends FlatSpec with SparkSessionTestWrapper with DataFrameComparer {

  behavior of "RequiredColProcessingSpec"

  it should "compute the required col processing in service classifier pre-processing" in {


    implicit val sp: SparkSession = spark


    val inputPath = getClass.getResource("/input_data/required_col_processing_input.txt").getPath
    val outputPath = getClass.getResource("/expected_data/required_col_processing_expected.txt").getPath

    val inputDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(inputPath)

    val requiredColProcessing = new RequiredColProcessing
    val resultDF = requiredColProcessing.process(inputDF).select("serviceId_title_description")
    val expectedDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(outputPath).select("serviceId_title_description")
    assertSmallDataFrameEquality(resultDF, expectedDF)

  }

}
