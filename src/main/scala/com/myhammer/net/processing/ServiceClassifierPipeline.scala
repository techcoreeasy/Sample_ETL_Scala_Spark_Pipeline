package com.myhammer.net.processing

import com.myhammer.net.pipeline.Pipeline
import com.myhammer.net.stages._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait ServiceClassifierPipeline {

  /** A ServiceClassifierPipeline trait which create pipeline for pre-processing */

  def _spark: SparkSession

  val pipeline: Pipeline[DataFrame, DataFrame] =
    Pipeline() :+
      new RequiredColSelection :+
      new PreConditionFilter :+
      new RequiredColProcessing :+
      StopWordRemover("serviceId_title_description", _spark) :+
      RemoveNonGermanLetter("serviceId_title_description") :+
      RemoveDuplicateWord("serviceId_title_description")

  def transform(data: DataFrame): DataFrame = pipeline.process(data)

}
