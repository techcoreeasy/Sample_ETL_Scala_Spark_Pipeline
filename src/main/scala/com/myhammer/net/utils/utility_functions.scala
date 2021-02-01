package com.myhammer.net.utils


import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

object utility_functions {


  def stopWordsRemover(inputDF: DataFrame, colName: String, stopWords: Array[String]): DataFrame = {

    /** A stopWordsRemover Transform DataFrame Row with removing stop words from it.
      *
      * @inputDF type of DataFrame
      * @colName type of String
      * @stopWords type of Array
      * @return type of DataFrame
      */


    val stopWordsRemoverFunction = new StopWordsRemover()
      .setStopWords(stopWords)
      .setInputCol(colName)
      .setOutputCol(colName + "_new")

    stopWordsRemoverFunction.transform(inputDF).drop(colName).withColumnRenamed(colName + "_new", colName)

  }

  def removeDuplicate(inputDF: DataFrame, colName: String): DataFrame = {

    /** A removeDuplicate Transform DataFrame Row with removing duplicate words from it.
      *
      * @inputDF type of DataFrame
      * @colName type of String
      * @return type of DataFrame
      */

    val removeDuplicateFunction =
      udf((col: String) => {
        col.split(" ").toList.distinct.mkString(" ")
      }
      )

    inputDF.withColumn(colName, removeDuplicateFunction(col(colName)))

  }

  def removeNonGermanLetter(inputDF: DataFrame, colName: String): DataFrame = {

    /** A removeNonGermanLetter Transform DataFrame Row with removing non german letter from it.
      *
      * @inputDF type of DataFrame
      * @colName type of String
      * @return type of DataFrame
      */

    val removeNonGermanLetterFunction =
      udf((col: String) => {
        col.replaceAll("[^0-9a-zA-ZäöüßÄÖÜẞ _]", "").replaceAll("\\s{2,}", " ").trim
      }
      )

    inputDF.withColumn(colName, removeNonGermanLetterFunction(col(colName)))

  }

  def tokenizerFunction(inputDF: DataFrame, colName: String): DataFrame = {

    /** A tokenizerFunction Transform DataFrame with tokenize each Row of DataFrame .
      *
      * @inputDF type of DataFrame
      * @colName type of String
      * @return type of DataFrame
      */

    val tokenizer = new Tokenizer().setInputCol(colName).setOutputCol(colName + "_new")

    tokenizer.transform(inputDF).drop(colName).withColumnRenamed(colName + "_new", colName)

  }

  def setNullableStateOfColumn(inputDF: DataFrame, colName: String, nullable: Boolean): DataFrame = {

    /** A setNullableStateOfColumn Transform DataFrame Column with type of nullable property either True or False.
      *
      * @df type of DataFrame
      * @cn type of String
      * @nullable type of Boolean
      * @return type of DataFrame
      */

    val schema = inputDF.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) if c.equals(colName) => StructField(c, t, nullable = nullable, m)
      case y: StructField => y
    })

    inputDF.sqlContext.createDataFrame(inputDF.rdd, newSchema)

  }

}
