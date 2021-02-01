package com.myhammer.net.stages

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.myhammer.net.conf.SparkSessionTestWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class RequiredColSelectionSpec extends FlatSpec with SparkSessionTestWrapper with DataFrameComparer {

  behavior of "RequiredColSelectionSpec"

  it should "compute the required col selection in service classifier pre-processing" in {


    implicit val sp: SparkSession = spark


    val inputPath = getClass.getResource("/input_data/required_col_selection_input.txt").getPath
    val outputPath = getClass.getResource("/expected_data/required_col_selection_expected.txt").getPath

    val inputDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(inputPath)

    val requiredColSelection = new RequiredColSelection
    val resultDF = requiredColSelection.process(inputDF)
    val expectedDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(outputPath)
    assertSmallDataFrameEquality(resultDF, expectedDF)

  }

}
