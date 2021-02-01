package com.myhammer.net.pipeline

import org.scalatest.FlatSpec


class PipelineSpec extends FlatSpec {

  behavior of "Pipeline"

  it should "allow to chain functions" in {
    val result = (Pipeline((x: Int) => x) pipe (g => g + 1)).process(1)
    assert(result === 2)
  }

  it should "allow to chain processing" in {
    val result = (Pipeline(Stage((x: Int) => x)) pipe Stage((g: Int) => g + 3)).process(1)
    assert(result === 4)
  }

  it should "allow to chain pipelines" in {
    val pipeline = Pipeline() pipe ((x: Int) => x + 2) pipe Pipeline((x: Int) => x - 1)
    val result = pipeline.process(1)
    assert(result === 2)
  }

  class AddTwo extends Stage[Int, Int] {
    override def process(in: Int): Int = in + 2

    override def toString: String = "AddTwo"
  }

  it should "have toString representation when filled with stages" in {
    assert(Pipeline(new AddTwo).pipe(new AddTwo).toString == "Pipeline(AddTwo -> AddTwo)")
  }

  it should "have toString representation when pipeline is empty" in {
    assert(Pipeline().toString == "Pipeline()")
  }

  it should "allow alias operations" in {
    val p1 = Pipeline() :+ ((x: Int) => x + 2) :+ Stage((g: Int) => g.toString + "A")
    val p2 = Pipeline() -> ((x: Int) => x + 2) -> Pipeline((g: Int) => g.toString + "A")

    assert(p1.process(1) === "3A")
    assert(p2.process(1) === "3A")

  }
}
