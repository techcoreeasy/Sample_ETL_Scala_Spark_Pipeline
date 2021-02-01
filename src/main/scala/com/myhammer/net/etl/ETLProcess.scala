package com.myhammer.net.etl

/** A generic ETL (Extract, Transform, Load) process that extracts data of type IN, transform it to OUT and load it.
  * @tparam IN type of data before the transformation, i.e. data extracted
  * @tparam OUT type of data after the transformation, i.e. data loaded
  * @tparam RES type of result after the load, e.g. summary statistics, failure or success code and so on
  */
trait ETLProcess[IN, OUT, RES] {

  /** Read the data from a source
    * @return data read
    */
  def extract(): IN

  /** Transform the input data into output data
    * @param in input data
    * @return output data
    */
  def transform(in: IN): OUT

  /** Writes the data somewhere
    * @param out output data written
    * @return the result of the operation
    */
  def load(out: OUT): RES

  /** Executes the process
    */
  def run(): RES = load(transform(extract()))
}

/** Factory for [[ETLProcess]]
  */
object ETLProcess {

  /** Creates an [[ETLProcess]] out of an extract function, a transform function and a load function.
    *
    * @param e extract function
    * @param t transform function
    * @param l load function
    * @tparam T type of data extracted
    * @tparam U type of data loaded
    * @tparam R of the operation
    * @return the [[ETLProcess]]
    */
  def apply[T, U, R](e: => T, t: T => U, l: U => R): ETLProcess[T, U, R] =
    new ETLProcess[T, U, R] {

      def extract(): T = e

      def transform(in: T): U = t(in)

      def load(out: U): R = l(out)
    }
}
