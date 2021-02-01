package com.myhammer.net.stages

import com.myhammer.net.pipeline.Stage
import com.myhammer.net.utils.utility_functions.tokenizerFunction
import org.apache.spark.sql.DataFrame

case class Tokenizer(colName: String) extends Stage[DataFrame, DataFrame] {

  override def process(input: DataFrame): DataFrame = {

    /** A Tokenizer Transform DataFrame with tokenize each Row of DataFrame .
      *
      * @input type of DataFrame
      * @return type of DataFrame
      */

    tokenizerFunction(input, colName)

  }

}
