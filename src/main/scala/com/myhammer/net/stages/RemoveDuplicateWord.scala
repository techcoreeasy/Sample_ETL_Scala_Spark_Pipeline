package com.myhammer.net.stages

import com.myhammer.net.pipeline.Stage
import com.myhammer.net.utils.utility_functions.removeDuplicate
import org.apache.spark.sql.DataFrame

case class RemoveDuplicateWord(colName: String) extends Stage[DataFrame, DataFrame] {

  override def process(input: DataFrame): DataFrame = {

    /** A RemoveDuplicateWord Transform process transform DataFrame with removing duplicate words from input DataFrame Row.
      *
      * @input type of DataFrame
      * @return type of DataFrame
      */

    removeDuplicate(input, colName)

  }

}
