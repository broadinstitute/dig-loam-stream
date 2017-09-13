package loamstream.wdl4s

import loamstream.util.Loggable
import org.scalatest.FunSuite
import wdl4s.wdl.WdlNamespaceWithWorkflow

/**
  * LoamStream
  * Created by oliverr on 6/12/2017.
  */
class Wdl4sTest extends FunSuite with Loggable {

  test("wdl4s") {
    val wdl = """
                |task a {
                |  command { ps }
                |}
                |workflow wf {
                | call a
                |}""".stripMargin

    val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get

    info("\n\n" + Wdl4sInspecter.inspectNameSpace(ns) + "\n")

  }

}
