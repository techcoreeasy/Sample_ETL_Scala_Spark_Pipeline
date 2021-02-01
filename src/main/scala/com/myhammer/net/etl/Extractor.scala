package com.myhammer.net.etl

/** Provides extract method
  * @tparam IN type of data extracted
  */
trait Extractor[IN] {
  def extract(): IN
}

/** Factory for [[Extractor]]
  */
object Extractor {

  /** Creates an [[Extractor]] out of a function
    * @param e the extract function
    * @tparam T the type of the data extracted
    * @return the [[Extractor]]
    */
  def apply[T](e: => T): Extractor[T] = new Extractor[T] {
    def extract(): T = e
  }
}
