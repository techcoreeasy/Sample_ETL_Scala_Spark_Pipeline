package com.myhammer.net.stages


import org.apache.spark.sql.SparkSession

case class PlaceHolderReplacement(spark: SparkSession) {

// TODO this stage is in WIP
  def process() = {


    val replacement_pattern = getClass.getResource("/unitmetrics-replacement-patterns.json").getPath

    val replacement_pattern_DF = (spark.read.text(replacement_pattern))

    replacement_pattern_DF.show()

    val myval = replacement_pattern_DF.rdd.map(row => row.get(0)).collect().toList

      myval.foreach( x => print(x))


  }

}





