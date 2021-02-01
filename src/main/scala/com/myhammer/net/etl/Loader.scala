package com.myhammer.net.etl

/** Provides load method
  * @tparam OUT type of data loaded
  */
trait Loader[OUT, RES] {
  def load(out: OUT): RES
}

/** Factory for [[Loader]]
  */
object Loader {

  /** Creates a [[Loader]] out of a load function
    * @param l load function
    * @tparam U type of data loaded
    * @return the [[Loader]]
    */
  def apply[U, R](l: U => R): Loader[U, R] = new Loader[U, R] {
    def load(out: U): R = l(out)
  }
}
