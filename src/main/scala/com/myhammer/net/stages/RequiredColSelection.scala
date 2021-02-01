package com.myhammer.net.stages

import com.myhammer.net.pipeline.Stage
import org.apache.spark.sql.DataFrame

class RequiredColSelection extends Stage[DataFrame, DataFrame] {

  override def process(input: DataFrame): DataFrame = {

    /** A RequiredColSelection Transform process transform DataFrame with selecting required column.
      *
      * @input type of DataFrame
      * @return type of DataFrame
      */

    input.select("serviceId", "zipcode", "city", "id", "title", "description", "createdAt", "service_selected_by_user")

  }


}
