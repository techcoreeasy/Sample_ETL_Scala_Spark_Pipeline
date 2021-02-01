package com.myhammer.net.stages

import com.myhammer.net.pipeline.Stage
import com.myhammer.net.utils.utility_functions.removeNonGermanLetter
import org.apache.spark.sql.DataFrame

case class RemoveNonGermanLetter(colName: String) extends Stage[DataFrame, DataFrame] {

  override def process(input: DataFrame): DataFrame = {

    /** A RemoveNonGermanLetter Transform process transform DataFrame with removing non German Letter from input DataFrame Row.
      *
      * @input type of DataFrame
      * @return type of DataFrame
      */

    removeNonGermanLetter(input, colName)

  }

}
