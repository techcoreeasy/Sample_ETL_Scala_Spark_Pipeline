package com.myhammer.net.pipeline

/** A stage of [[Pipeline]]. It is an abstraction of a generic computation, which can be applied with [[Stage#process]].
  *
  * @tparam IN  the input type of the computation
  * @tparam OUT the output type of the computation
  * @example {{{ class Plus1 extends Stage[Int, Int] { override def process(input: Int): Int = input + 1 }
  *  val pipeline = Pipeline().pipe(new Plus1) }}}
  */
trait Stage[-IN, +OUT] {

  /** Executes the computation with a given input
    *
    * @param in the input of the computation
    * @return the result of the computation
    */
  def process(in: IN): OUT
}

/** Factory for Stage
  */
object Stage {

  /** Creates a Pipeline from a function
    *
    * @param f the function
    * @tparam IN  the input type of the function
    * @tparam OUT the output type of the function
    * @return the stage
    */
  def apply[IN, OUT](f: IN => OUT): Stage[IN, OUT] = new Stage[IN, OUT] {
    def process(in: IN): OUT = f(in)
  }

  /** Implicit conversion from Stage to Function1
    *
    * @param s
    * @tparam IN
    * @tparam OUT
    * @return
    */
  implicit def stageToFunction[IN, OUT](s: Stage[IN, OUT]): IN => OUT = s.process
}
