package loamstream.cwl

import cats.data.Validated.{Invalid, Valid}
import lenthall.validation.ErrorOr.ErrorOr
import loamstream.LoamGraphExamples
import loamstream.loam.LoamGraph
import org.scalatest.FunSuite


/**
  * LoamStream
  * Created by oliverr on 9/19/2017.
  */
final class LoamToWomTest extends FunSuite {

  def errorOrToMessage[A](errorOrA: ErrorOr[A]): String = errorOrA match {
    case Valid(a) => s"Looks like a valid $a"
    case Invalid(messages) => messages.toList.mkString("\n", "\n", "\n")
  }

  private val loamGraph: LoamGraph = LoamGraphExamples.simple

  test("Convert toy Loam to WOM") {
    val errorOrWorkflow = LoamToWom.loamToWom("daGraph", loamGraph)
    assert(errorOrWorkflow.isValid, errorOrToMessage(errorOrWorkflow))
  }

  ignore("Convert analysis pipeline from Loam to WOM") {
    val graph = LoamGraphExamples.getAnalysisGraph
    val errorOrWorkflow = LoamToWom.loamToWom("analysis pipeline", graph)
    assert(errorOrWorkflow.isValid, errorOrToMessage(errorOrWorkflow))
  }


}
