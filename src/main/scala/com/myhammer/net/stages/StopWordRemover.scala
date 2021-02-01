package com.myhammer.net.stages

import com.myhammer.net.pipeline.Stage
import com.myhammer.net.utils.utility_functions.{stopWordsRemover, tokenizerFunction}
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.StringType

case class StopWordRemover(colName: String , spark : SparkSession) extends Stage[DataFrame, DataFrame] {

  val config = ConfigFactory.load("serviceclassifier.conf")

  val s3_STOPWORDS = config.getString("S3_STOPWORDS_PATH")

  override def process(input : DataFrame): DataFrame = {

    val stopwords_from_s3 = spark.sparkContext.textFile(s3_STOPWORDS).collect()
    val stopwords_from_scala_lib = StopWordsRemover.loadDefaultStopWords("german")
    val combined_stop_words = stopwords_from_s3 ++ stopwords_from_scala_lib

    val tokenizeDF = tokenizerFunction(input, colName)

    stopWordsRemover(tokenizeDF, colName, combined_stop_words)
      .withColumn(colName, concat_ws(" ", col(colName)).cast(StringType))
      .filter(col(colName).isNotNull)

  }


}
