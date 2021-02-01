package com.myhammer.net.stages

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.myhammer.net.conf.SparkSessionTestWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class RemoveNonGermanLetterSpec extends FlatSpec with SparkSessionTestWrapper with DataFrameComparer {

  behavior of "RemoveDuplicateWordSpec"

  it should "compute the removal of non german words from col in service classifier pre-processing" in {


    implicit val sp: SparkSession = spark


    val inputPath = getClass.getResource("/input_data/remove_non_german_letter_input.txt").getPath
    val outputPath = getClass.getResource("/expected_data/remove_non_german_letter_expected.txt").getPath

    val inputDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(inputPath)

    val resultDF = RemoveNonGermanLetter("non_german_col").process(inputDF)

    val expectedDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(outputPath)

    assertSmallDataFrameEquality(resultDF, expectedDF)

  }

}
