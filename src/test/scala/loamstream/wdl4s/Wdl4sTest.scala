package loamstream.wdl4s

import loamstream.util.Loggable
import org.scalatest.FunSuite
import wdl4s.WdlNamespaceWithWorkflow

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

    info(s"Workflow: ${ns.workflow.unqualifiedName}")
    ns.workflow.calls foreach {call =>
      info(s"Call: ${call.unqualifiedName}")
    }

    ns.tasks foreach {task =>
      info(s"Task: ${task.name}")
      info(s"Command: ${task.commandTemplate}")
    }
  }

}
