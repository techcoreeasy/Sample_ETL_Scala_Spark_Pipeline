package com.myhammer.net.stages

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.myhammer.net.conf.SparkSessionTestWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class RemoveDuplicateWordSpec extends FlatSpec with SparkSessionTestWrapper with DataFrameComparer {

  behavior of "RemoveDuplicateWordSpec"

  it should "compute the duplicate words in service classifier pre-processing" in {


    implicit val sp: SparkSession = spark


    val inputPath = getClass.getResource("/input_data/remove_duplicate_word_input.txt").getPath
    val outputPath = getClass.getResource("/expected_data/remove_duplicate_word_expected.txt").getPath

    val inputDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(inputPath)

    val resultDF = RemoveDuplicateWord("duplicate_word_col").process(inputDF)

    val expectedDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(outputPath)

    assertSmallDataFrameEquality(resultDF, expectedDF)

  }

}
