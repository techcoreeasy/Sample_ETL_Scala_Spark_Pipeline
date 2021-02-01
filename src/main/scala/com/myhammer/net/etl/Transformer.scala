package com.myhammer.net.etl

/** Provides the transform method
  * @tparam IN type of data before the transformation
  * @tparam OUT type of data after the transformation
  */
trait Transformer[IN, OUT] {
  def transform(in: IN): OUT
}

/** Factory for [[Transformer]]
  */
object Transformer {

  /** Creates a [[Transformer]] out of a function
    * @param t the transform function
    * @tparam T the type of the data before the transformation
    * @tparam U the type of the data after the transformation
    * @return the [[Transformer]]
    */
  def apply[T, U](t: T => U): Transformer[T, U] = new Transformer[T, U] {
    def transform(in: T): U = t(in)
  }
}
