package com.myhammer.net.stages

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.myhammer.net.conf.SparkSessionTestWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec


class PreConditionFilterSpec extends FlatSpec with SparkSessionTestWrapper with DataFrameComparer {

  behavior of "PreConditionFilterSpec"

  it should "compute the pre condition filter of pre-processing of service classifier pre-processing" in {


    implicit val sp: SparkSession = spark


    val inputPath = getClass.getResource("/input_data/precondition_filter_input.txt").getPath
    val outputPath = getClass.getResource("/expected_data/precondition_filter_expected.txt").getPath

    val inputDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(inputPath)
    val preConditionFilter = new PreConditionFilter
    val resultDF = preConditionFilter.process(inputDF)

    val expectedDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(outputPath)

    assertSmallDataFrameEquality(resultDF, expectedDF)

  }

}
