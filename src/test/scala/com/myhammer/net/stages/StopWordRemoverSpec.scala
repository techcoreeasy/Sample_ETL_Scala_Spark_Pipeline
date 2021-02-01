package com.myhammer.net.stages

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.myhammer.net.conf.SparkSessionTestWrapper
import com.myhammer.net.utils.utility_functions.setNullableStateOfColumn
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class StopWordRemoverSpec extends FlatSpec with SparkSessionTestWrapper with DataFrameComparer {

  behavior of "StopWordRemoverSpec"

  it should "compute the stop word remover in service classifier pre-processing" in {


    implicit val sp: SparkSession = spark


    val config = ConfigFactory.load("serviceclassifier.conf")

    val S3_STOPWORDS_FILE_PATH = config.getString("S3_STOPWORDS_PATH")

    val S3StopWords = spark.sparkContext.textFile(S3_STOPWORDS_FILE_PATH).collect()

    val inputPath = getClass.getResource("/input_data/stop_word_remover_input.txt").getPath
    val outputPath = getClass.getResource("/expected_data/stop_word_remover_expected.txt").getPath

    val inputDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(inputPath)

    val resultDF = StopWordRemover("serviceId_title_description", spark).process(inputDF)
    resultDF.printSchema()

    val expectedDF = sp.read.option("delimiter", "|").option("header", true).option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true).csv(outputPath)
    val expectedDF_with_schema = setNullableStateOfColumn(expectedDF, "serviceId_title_description", false)
    assertSmallDataFrameEquality(resultDF, expectedDF_with_schema)

  }

}
