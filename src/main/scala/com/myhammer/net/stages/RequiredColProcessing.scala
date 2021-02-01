package com.myhammer.net.stages

import com.myhammer.net.pipeline.Stage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, udf}

class RequiredColProcessing extends Stage[DataFrame, DataFrame] {

  override def process(input: DataFrame): DataFrame = {

    /** A RequiredColProcessing Transform process transform DataFrame with basic processing.
      *
      * @input type of DataFrame
      * @return type of DataFrame
      */

    val removeZipCityFunction =
      udf((col: String, zipCode: Int, city: String) => {
        col.replaceAllLiterally(zipCode.toString, "").replaceAllLiterally(city, "");
      }
      )

    input.withColumn("title_description",
      concat(col("title"), lit(" "), col("description")))
      .drop("title", "description")
      .withColumn("title_description",
        removeZipCityFunction(col("title_description"), col("zipcode"), col("city")))
      .withColumn("serviceId_title_description",
        concat(lit("__label__"), col("serviceId"), lit(" "), col("title_description")))
      .select("serviceId_title_description")

  }


}
