package com.myhammer.net.stages

import com.myhammer.net.pipeline.Stage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length, year}

class PreConditionFilter extends Stage[DataFrame, DataFrame] {

  override def process(input: DataFrame): DataFrame = {

    /** A PreConditionFilter Transform process transform DataFrame with conditional filter.
      * @input type of DataFrame
      * @return type of DataFrame
      */

    input.filter(year(col("createdAt")) >= "2016")
      .filter(col("serviceId").isNotNull
        && col("service_selected_by_user") === 1
        && length(col("serviceId")) >= 5)

  }

}
